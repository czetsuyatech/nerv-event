package com.czetsuyatech.nerv.event.core.retention;

/**
 * <p>
 * Optional cleanup port for persistence sidecars which can safely be removed once unreferenced.
 * </p>
 *
 * <p>
 * The core does not attach any domain meaning to a sidecar. Its persistence adapter owns the reference checks and must
 * perform them in the guarded delete operation.
 * </p>
 */
public interface RetentionSidecarCleaner {

  /**
   * Deletes at most {@code limit} currently unreferenced sidecar rows.
   */
  int deleteUnreferenced(int limit);
}
