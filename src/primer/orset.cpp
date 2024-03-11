#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  for (int i = 0; i < e_.size(); i++) {
    if (e_[i].first == elem) {
      return true;
    }
  }
  return false;
  //
  throw NotImplementedException("ORSet<T>::Contains is not implemented");
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  e_.push_back(std::make_pair(elem, uid));
  int i = 0;
  while (i < e_.size())  // E\B.T
  {
    bool flag = false;
    for (int j = 0; j < t_.size(); j++) {
      if (t_[j] == e_[i]) {
        e_.erase(e_.begin() + i);
        flag = true;
      }  // delete done
    }
    if (!flag) {
      i++;
    }
  }
  return;
  //
  throw NotImplementedException("ORSet<T>::Add is not implemented");
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  int i = 0;
  while (i < e_.size()) {
    if (e_[i].first == elem) {
      bool flag = true;
      for (int j = 0; j < t_.size(); j++)  // have been added to T
      {
        if (t_[j] == e_[i]) {
          flag = false;
          break;
        }
      }
      if (flag) {
        t_.push_back(std::make_pair(e_[i].first, e_[i].second));
      }
      e_.erase(e_.begin() + i);
    } else {
      i++;
    }
  }
  //
  return;
  throw NotImplementedException("ORSet<T>::Remove is not implemented");
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  int i = 0;
  while (i < e_.size())  // E\B.T
  {
    bool flag = false;
    for (int j = 0; j < other.t_.size(); j++) {
      if (other.t_[j] == e_[i]) {
        flag = true;
        e_.erase(e_.begin() + i);
      }  // delete from E
    }
    if (!flag) {
      i++;
    }  // haven't delete anything
  }
  for (int i = 0; i < other.e_.size(); i++)  // add B.E\T
  {
    bool flag = true;
    for (int j = 0; j < t_.size(); j++) {
      if (other.e_[i] == t_[j]) {
        flag = false;
        break;
      }  // in T,cannot add
    }
    for (int j = 0; j < e_.size(); j++) {
      if (other.e_[i] == e_[j])  // already contained,cannot add
      {
        flag = false;
        break;
      }
    }
    if (flag) {
      e_.push_back(other.e_[i]);
    }
  }
  for (int i = 0; i < other.t_.size(); i++)  // T=T||B.T
  {
    bool flag = true;
    for (int j = 0; j < t_.size(); j++) {
      if (t_[j] == other.t_[i]) {
        flag = false;
        break;
      }  // already contained
    }
    if (flag) {
      t_.push_back(other.t_[i]);
    }
  }

  //
  return;
  throw NotImplementedException("ORSet<T>::Merge is not implemented");
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  std::vector<T> s;
  for (int i = 0; i < e_.size(); i++) {
    bool flag = false;
    for (int j = 0; j < s.size(); j++) {
      if (e_[i].first == s[j]) {
        flag = true;
        break;
      }  // already contained
    }
    if (!flag) {
      s.push_back(e_[i].first);
    }
  }

  return s;
  //
  throw NotImplementedException("ORSet<T>::Elements is not implemented");
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
