package server.config;

/**
 * Utility class for validating broker configuration values
 */
public final class BrokerConfigValidator {

    private BrokerConfigValidator() {}

    /**
     * Validate that a value falls within a specified range
     *
     * @param value The value to validate
     * @param min Minimum allowed value (inclusive)
     * @param max Maximum allowed value (inclusive)
     * @param fieldName Name of the field being validated
     * @param unit Unit of measurement (e.g., "ms", "") for error messages
     * @throws IllegalArgumentException if value is out of range
     */
    public static void validateRange(long value, long min, long max, String fieldName, String unit) {
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                String.format("%s must be between %d%s and %d%s", fieldName, min, unit, max, unit)
            );
        }
    }
}
