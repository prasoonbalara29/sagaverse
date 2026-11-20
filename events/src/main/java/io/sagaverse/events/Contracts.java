package io.sagaverse.events;

/** Argument checks shared by the contract records. Package-private: not part of the API. */
final class Contracts {

  private Contracts() {}

  static String requireNonBlank(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  static int requirePositive(int value, String name) {
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive, was " + value);
    }
    return value;
  }
}
