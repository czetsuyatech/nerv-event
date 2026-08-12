# Nerv Event Agent Rules

## Build Baseline

- Use Java 21 or later and Maven 3.8.1 or later.
- Run Maven commands from the repository root so the complete reactor is included.

## Java Formatting

- Use the Eclipse formatter configuration at `config/eclipse-formatter.xml` for Java source files. Do not hand-format Java in a way that conflicts with that configuration.
- After modifying Java, run `mvn formatter:format`; verify the result with `mvn formatter:validate`.
- The formatter uses two-space indentation, spaces instead of tabs, and a 120-character line limit. Preserve at most one consecutive blank line.
- Keep method-declaration braces on the same line as the declaration, as defined by the Eclipse formatter.
- Leave one blank line only between consecutive annotated field declarations, such as `@NonNull` fields. Do not add blank lines between unannotated fields. Eclipse cannot enforce this condition natively, so apply it manually after formatting.
- Keep a method invocation on one line when all of its arguments are single-character identifiers, regardless of argument count. Do not wrap it merely because it has multiple parameters; for example: `method(a, b, c, d);`.
- Do not apply the Java formatter to CSS, HTML, JavaScript, JSON, or XML files; the Maven formatter intentionally skips them.
