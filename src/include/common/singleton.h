#ifndef MINISQL_SINGLETON_H
#define MINISQL_SINGLETON_H

#include "common/macros.h"

template <typename T>
class Singleton {
 public:
  DISALLOW_COPY(Singleton);
  static T &getInstance() {
    static T instance;
    return instance;
  }
  virtual ~Singleton() = default;

 protected:
  Singleton() = default;
};

#endif  // MINISQL_SINGLETON_H
