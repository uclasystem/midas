#pragma once

namespace midas {

template <class IntType, class RealType>
inline zipf_table_distribution<IntType, RealType>::zipf_table_distribution(
    const IntType n, const RealType q)
    : n_(init(n, q)), q_(q), dist_(pdf_.begin(), pdf_.end()) {}

template <class IntType, class RealType>
inline void zipf_table_distribution<IntType, RealType>::reset() {}

template <class IntType, class RealType>
inline IntType
zipf_table_distribution<IntType, RealType>::operator()(std::mt19937 &rng) {
  return dist_(rng) - 1;
}

template <class IntType, class RealType>
inline RealType zipf_table_distribution<IntType, RealType>::s() const {
  return q_;
}

template <class IntType, class RealType>
inline IntType
zipf_table_distribution<IntType, RealType>::min() const {
  return 0;
}

template <class IntType, class RealType>
inline IntType
zipf_table_distribution<IntType, RealType>::max() const {
  return n_ - 1;
}

template <class IntType, class RealType>
inline IntType
zipf_table_distribution<IntType, RealType>::init(const IntType n,
                                                 const RealType q) {
  pdf_.reserve(n + 1);
  pdf_.emplace_back(0.0);
  for (IntType i = 1; i <= n; i++) {
    pdf_.emplace_back(std::pow((double)i, -q));
  }
  return n;
}
} // namespace midas
