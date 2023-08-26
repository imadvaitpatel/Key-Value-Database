#include <exception>
#include <string>

using namespace std;

class KeyException : public exception {
  private:
    string message;
  public:
    KeyException(string msg) : message(msg) {}
    const char *what() const noexcept override {
      return message.c_str();
    }
};

class NotImplementedException : public exception {
  private:
    string message;
  public:
    NotImplementedException(string msg) : message(msg) {}
    const char *what() const noexcept override {
      return message.c_str();
    }
};
