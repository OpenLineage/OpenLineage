/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.utils;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import lombok.SneakyThrows;

/** Class used to generate UUID values. */
public class UUIDUtils {
  // SecureRandom return truly random values, but it is 4x slower than Random.
  private static final SecureRandom TRUE_RANDOM = new SecureRandom();
  // Using ThreadLocal to avoid lock contention in multithread environment.
  private static final ThreadLocal<Random> RANDOM =
      new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
          // Random is pseudo-random, so it has to be initialized with a random seed
          // to avoid producing same results in a different thread
          long seed = TRUE_RANDOM.nextLong();
          return new Random(seed);
        }
      };

  /**
   * Generate new UUID. Each function call returns a new UUID value.
   *
   * <p>UUID version is an implementation detail, and <b>should not</b> be relied on. For now it is
   * <a href="https://datatracker.ietf.org/doc/rfc9562/">UUIDv7</a>, so for increasing instant
   * values, returned UUID is always greater than previous one.
   *
   * @return {@link UUID} v7
   * @since 1.15.0
   */
  public static UUID generateNewUUID() {
    return generateNewUUID(Instant.now());
  }

  /**
   * Generate new UUID for an instant of time. Each function call returns a new UUID value.
   *
   * <p>UUID version is an implementation detail, and <b>should not</b> be relied on. For now it is
   * <a href="https://datatracker.ietf.org/doc/rfc9562/">UUIDv7</a>, so for increasing instant
   * values, returned UUID is always greater than previous one.
   *
   * <p>Based on <a
   * href="https://github.com/f4b6a3/uuid-creator/blob/98628c29ac9da704d215245f2099d9b439bc3084/src/main/java/com/github/f4b6a3/uuid/UuidCreator.java#L584-L593"
   * target="_top">com.github.f4b6a3.uuid.UuidCreator.getTimeOrderedEpoch</a> implementation (MIT
   * License).
   *
   * @param instant a given instant
   * @return {@link UUID} v7
   * @since 1.15.0
   */
  public static UUID generateNewUUID(Instant instant) {
    long time = instant.toEpochMilli();
    Random random = RANDOM.get();
    long msb = (time << 16) | (random.nextLong() & 0x0fffL) | 0x7000L;
    long lsb = (random.nextLong() & 0x3fffffffffffffffL) | 0x8000000000000000L;
    return new UUID(msb, lsb);
  }

  /**
   * Generate UUID for instant of time and input data. Calling function with same arguments always
   * produces the same result.
   *
   * <p>UUID version is an implementation detail, and <b>should not</b> be relied on. For now it is
   * <a href="https://datatracker.ietf.org/doc/rfc9562/">UUIDv7</a>, so for increasing instant
   * values, returned UUID is always greater than previous one. The only difference from RFC 9562 is
   * that least significant bytes are not random, but instead a SHA-1 hash of input data.
   *
   * <p>Based on <a
   * href="https://github.com/f4b6a3/uuid-creator/blob/98628c29ac9da704d215245f2099d9b439bc3084/src/main/java/com/github/f4b6a3/uuid/UuidCreator.java#L584-L593"
   * target="_top">com.github.f4b6a3.uuid.UuidCreator.getTimeOrderedEpoch</a> implementation (MIT
   * License).
   *
   * @param instant a given instant
   * @param data a given data
   * @return {@link UUID} v7
   * @since 1.32.0
   */
  @SneakyThrows
  public static UUID generateStaticUUID(Instant instant, byte[] data) {
    MessageDigest md = MessageDigest.getInstance("SHA-1");
    // if data is some static value, e.g. job name, mix it with instant to make it more random
    md.update(instant.toString().getBytes("UTF-8"));
    md.update(data);

    byte[] hash = md.digest();
    long hasbMsb = 0;
    long hasbLsb = 0;
    for (int i = 0; i < 8; i++) hasbMsb = (hasbMsb << 8) | (hash[i] & 0xff);
    for (int i = 8; i < 16; i++) hasbLsb = (hasbLsb << 8) | (hash[i] & 0xff);

    long time = instant.toEpochMilli();
    long uuidMsb = (time << 16) | (hasbMsb & 0x0fffL) | 0x7000L;
    long uuidLsb = (hasbLsb & 0x3fffffffffffffffL) | 0x8000000000000000L;
    return new UUID(uuidMsb, uuidLsb);
  }
}
