#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/errno.h>
#include <linux/cdev.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/uaccess.h>
#include <linux/sched/mm.h>
#include <asm/tlbflush.h>

#include "koord.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Yifan Qiao");
MODULE_DESCRIPTION("Midas Coordinator Module");

#ifndef HUGEPAGE_SIZE
#define HUGEPAGE_SIZE (512ul * 4096) // 2MB
#define HUGEPAGE_MASK (HUGEPAGE_SIZE - 1)
#endif // HUGEPAGE_SIZE

#ifndef HUGEPAGE_SHIFT
#define HUGEPAGE_SHIFT (9 + 12) // 2^21
#endif // HUGEPAGE_SHIFT

/* the character device that provides the koord IOCTL interface */
static struct cdev koord_cdev;

/**
 * ksched_lookup_task - retreives a task from a pid number
 * @nr: the pid number
 *
 * WARNING: must be called inside an RCU read critical section.
 *
 * Returns a task pointer or NULL if none was found.
 */
static struct task_struct *koord_lookup_task(pid_t nr)
{
	return pid_task(find_vpid(nr), PIDTYPE_PID);
}

void mm_trace_rss_stat(struct mm_struct *mm, int member, long count) {}

static int map_zero_page(pte_t *pte, unsigned long addr, void *arg)
{
	struct mm_struct *mm = (struct mm_struct *)arg;
	struct page *page = alloc_page(GFP_KERNEL);
	pte_t entry;
	if (!page)
		return -ENOMEM;
	entry = mk_pte(page, PAGE_SHARED);
	set_pte(pte, entry);
	inc_mm_counter(mm, MM_ANONPAGES);
	/* on x86 update_mmu_cache is nop */
	// update_mmu_cache(/* vma = */ NULL, addr, pte);
	return 0;
}

static int unmap_page(pte_t *pte, unsigned long addr, void *arg)
{
	struct mm_struct *mm = (struct mm_struct *)arg;
	struct page *page = pte_page(*pte);
	put_page(page);
	pte_clear(NULL, 0, pte);
	pte_unmap(pte);
	dec_mm_counter(mm, MM_ANONPAGES);
	return 0;
}

static int koord_cleanup_pmd(struct mm_struct *mm, unsigned long address)
{
	pgd_t *pgd;
	p4d_t *p4d;
	pud_t *pud;
	pmd_t *pmd;

	pgd = pgd_offset(mm, address);
	if (unlikely(pgd_none(*pgd) || pgd_bad(*pgd)))
		return -EINVAL;
	p4d = p4d_offset(pgd, address);
	if (unlikely(p4d_none(*p4d) || p4d_bad(*p4d)))
		return -EINVAL;
	pud = pud_offset(p4d, address);
	if (unlikely(pud_none(*pud)) || pud_bad(*pud))
		return -EINVAL;
	pmd = pmd_offset(pud, address);
	if (unlikely(pmd_none(*pmd)))
		return -EINVAL;
	pmd_clear(pmd);
	mm_dec_nr_ptes(mm);
	// pud_clear(pud);
	// mm_dec_nr_ptes(mm);
	// p4d_clear(p4d);
	// mm_dec_nr_ptes(mm);
	return 0;
}

static int koord_register(pid_t pid)
{
	int ret = 0;
	struct task_struct *p;
	rcu_read_lock();
	p = koord_lookup_task(pid);
	if (!p)
		ret = -ESRCH;
	rcu_read_unlock();
	return ret;
}

static int koord_unregister(pid_t pid)
{
	return 0;
}

static int koord_map_regions(struct koord_region_req __user *ureq)
{
	int ret = 0;
	int i;
	struct task_struct *t;
	struct mm_struct *mm;
	struct koord_region_req req;
	ret = copy_from_user(&req, ureq, sizeof(req));
	if (unlikely(ret))
		return ret;

	rcu_read_lock();
	t = koord_lookup_task(req.pid);
	if (!t) {
		rcu_read_unlock();
		return -ESRCH;
	}
	mm = get_task_mm(t);
	rcu_read_unlock();
	if (!mm)
		goto fail_mmput;

	for (i = 0; i < req.nr; i++) {
		uint64_t addr;

		ret = copy_from_user(&addr, &ureq->addrs[i], sizeof(addr));
		if (unlikely(ret))
			goto fail_mmput;
		if (addr & HUGEPAGE_MASK) {
			ret = -EINVAL;
			goto fail_mmput;
		}
		ret = apply_to_page_range(mm, addr, req.region_size,
					  map_zero_page, mm);
		if (ret)
			goto fail_mmput;
	}

fail_mmput:
	mmput(mm);
	__flush_tlb_all();
	return ret;
}

static int koord_unmap_regions(struct koord_region_req __user *ureq)
{
	int ret = 0;
	int i;
	struct task_struct *t;
	struct mm_struct *mm;
	struct koord_region_req req;
	ret = copy_from_user(&req, ureq, sizeof(req));
	if (unlikely(ret))
		return ret;

	rcu_read_lock();
	t = koord_lookup_task(req.pid);
	if (!t) {
		rcu_read_unlock();
		return -ESRCH;
	}
	mm = get_task_mm(t);
	rcu_read_unlock();
	if (!mm)
		goto fail_mmput;

	for (i = 0; i < req.nr; i++) {
		uint64_t addr;
		ret = copy_from_user(&addr, &ureq->addrs[i], sizeof(addr));
		if (unlikely(ret))
			goto fail_mmput;
		if (addr & HUGEPAGE_MASK) {
			ret = -EINVAL;
			goto fail_mmput;
		}

		ret = apply_to_page_range(mm, addr, req.region_size, unmap_page,
					  mm);
		if (unlikely(ret))
			goto fail_mmput;
		ret = koord_cleanup_pmd(mm, addr);
		if (unlikely(ret))
			goto fail_mmput;
	}

fail_mmput:
	mmput(mm);
	__flush_tlb_all();
	return ret;
}

static long koord_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	int ret = 0;
	/* validate input */
	if (unlikely(_IOC_TYPE(cmd) != KOORD_MAGIC))
		return -ENOTTY;
	if (unlikely(_IOC_NR(cmd) > KOORD_IOC_MAXNR))
		return -ENOTTY;

	switch (cmd) {
	case KOORD_REG:
		ret = koord_register((pid_t)arg);
		break;
	case KOORD_UNREG:
		ret = koord_unregister((pid_t)arg);
		break;
	case KOORD_MAP:
		ret = koord_map_regions((struct koord_region_req __user *)arg);
		break;
	case KOORD_UNMAP:
		ret = koord_unmap_regions(
			(struct koord_region_req __user *)arg);
		break;
	default:
		return -EINVAL;
	}
	return ret;
}

static int koord_open(struct inode *inode, struct file *filp)
{
	return 0;
}

static int koord_release(struct inode *inode, struct file *filp)
{
	return 0;
}

static struct file_operations koord_ops = {
	.owner = THIS_MODULE,
	.unlocked_ioctl = koord_ioctl,
	.open = koord_open,
	.release = koord_release,
};

static int __init koord_init(void)
{
	dev_t devno = MKDEV(KOORD_MAJOR, KOORD_MINOR);
	int ret;

	ret = register_chrdev_region(devno, 1, "koord");
	if (ret) {
		pr_err("Failed to allocate character device region\n");
		return ret;
	}

	cdev_init(&koord_cdev, &koord_ops);
	ret = cdev_add(&koord_cdev, devno, 1);
	if (ret) {
		pr_err("Failed to add character device\n");
		unregister_chrdev_region(devno, 1);
		return -1;
	}

	pr_info("koord: dev ready\n");
	return 0;
}

static void __exit koord_exit(void)
{
	dev_t devno = MKDEV(KOORD_MAJOR, KOORD_MINOR);

	// Unregister the character device
	cdev_del(&koord_cdev);
	unregister_chrdev_region(devno, 1);

	pr_info("koord: unloaded\n");
}

module_init(koord_init);
module_exit(koord_exit);