#pragma once
#include <vector>
#include <numeric>
#include <cstdlib>

template<typename T, typename U>
void quickSortWithProperties(size_t left, size_t right, std::vector<T>& vec1, std::vector<U>& vec2) {
    if (left >= right) return;

    size_t i = left, j = right;
    T pivot = vec1[(left + right) / 2];

    while (i <= j) {
        while (vec1[i] < pivot) i++;
        while (vec1[j] > pivot) j--;

        if (i <= j) {
            std::swap(vec1[i], vec1[j]);
            std::swap(vec2[i], vec2[j]);
            i++;
            if (j > 0) j--; // To avoid underflow
        }
    }

    if (left < j) {
        quickSortWithProperties(left, j, vec1, vec2);
    }
    if (i < right) {
        quickSortWithProperties(i, right, vec1, vec2);
    }
}

///@brief Sort two vectors with the same order
template<typename T, typename U>
void vec_sort(std::vector<T> &vec1, std::vector<U>& vec2) {
    assert(vec1.size() == vec2.size());
    if (!vec1.empty()) {
        quickSortWithProperties(0, vec1.size() - 1, vec1, vec2);
    }
}
