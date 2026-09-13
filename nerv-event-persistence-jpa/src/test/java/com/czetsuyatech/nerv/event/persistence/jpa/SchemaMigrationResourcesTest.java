package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Guards the migration resources that must be included in the published persistence JAR.
 */
class SchemaMigrationResourcesTest {

  static final List<String> MIGRATIONS = List.of(
      "001-create-outbox.sql",
      "002-create-inbox.sql",
      "003-create-trace-context.sql",
      "004-create-indexes.sql",
      "005-add-outbox-claim-version.sql",
      "006-add-outbox-ordering-key.sql"
  );

  @Test
  void canonicalPostgresqlMigrationsArePackagedAtTheirNonFlywayLocation() {
    for (String migration : MIGRATIONS) {
      String resource = "META-INF/nerv-event/db/postgresql/migration/" + migration;
      InputStream stream = getClass().getClassLoader().getResourceAsStream(resource);
      assertThat(stream).as(resource).isNotNull();
    }
  }
}
