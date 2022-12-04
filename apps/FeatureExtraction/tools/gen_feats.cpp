#include <fstream>
#include <iostream>
#include <string>

#include "../inc/constants.hpp"

namespace FeatExt {
int gen_fake_feats(int nr_imgs) {
  std::ofstream feat_file("fake_feat_vec.data", std::ofstream::binary);
  float *feats = new float[nr_imgs * kFeatDim];

  for (int i = 0; i < nr_imgs; i++)
    for (int j = 0; j < kFeatDim; j++)
      feats[i * kFeatDim + j] = static_cast<float>(j);

  if (feat_file.good()) {
    feat_file.write(reinterpret_cast<char *>(feats),
                    sizeof(float) * kFeatDim * nr_imgs);
  }

  return 0;
}
} // namespace FeatExt

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: ./gen_feats <nr_imgs>" << std::endl;
  }

  int nr_imgs = std::stoi(argv[1]);
  FeatExt::gen_fake_feats(nr_imgs);
  return 0;
}