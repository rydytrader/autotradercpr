package com.rydytrader.autotrader.util;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Cross-cutting file I/O helpers.
 *
 * Windows' {@link Files#move} with {@code ATOMIC_MOVE + REPLACE_EXISTING} occasionally throws
 * {@link AccessDeniedException} when AV / Explorer preview / another process briefly holds a
 * handle on the target. The failure is transient (milliseconds). These helpers add retry with
 * short backoff and a non-atomic fallback so cache writes don't spam the log with errors.
 */
public final class FileIoUtils {
    private FileIoUtils() {}

    /**
     * Atomically replace {@code dst} with {@code src}. Retries up to 5 times with linear
     * backoff (~1.5s total) on {@link AccessDeniedException}; falls back to delete + non-atomic
     * move if all retries fail. The atomic guarantee is lost only in the fallback path.
     */
    public static void atomicMoveWithRetry(Path src, Path dst) throws IOException {
        IOException lastErr = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                return;
            } catch (AccessDeniedException e) {
                lastErr = e;
                try { Thread.sleep(100L * (attempt + 1)); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        try {
            Files.deleteIfExists(dst);
            Files.move(src, dst);
        } catch (IOException e) {
            if (lastErr != null) e.addSuppressed(lastErr);
            throw e;
        }
    }
}
