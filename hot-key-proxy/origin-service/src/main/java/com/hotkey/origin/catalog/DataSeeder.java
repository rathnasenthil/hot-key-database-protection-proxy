package com.hotkey.origin.catalog;

import com.hotkey.origin.SeedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DataSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DataSeeder.class);
    private static final int REVIEW_BATCH = 100_000;

    private final JdbcTemplate jdbc;
    private final SeedProperties properties;
    private volatile boolean complete;
    private volatile String detail = "pending";
    private volatile long productCount;

    public DataSeeder(JdbcTemplate jdbc, SeedProperties properties) {
        this.jdbc = jdbc;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            tryEnableStatements();
            seed();
            complete = true;
        } catch (RuntimeException exception) {
            detail = exception.getMessage();
            log.error("Catalog seed failed", exception);
        }
    }

    private void tryEnableStatements() {
        try {
            jdbc.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        } catch (RuntimeException exception) {
            log.warn("pg_stat_statements extension was not created: {}", exception.getMessage());
        }
    }

    private void seed() {
        if (!properties.isEnabled()) {
            productCount = countProducts();
            detail = "disabled";
            log.info("Seed disabled, products={}", productCount);
            return;
        }
        try {
            jdbc.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
        } catch (RuntimeException exception) {
            log.info("pg_stat_statements is not installed on this server: {}", exception.getMessage());
        }
        long existing = countProducts();
        if (existing > 0) {
            productCount = existing;
            detail = "already-seeded";
            log.info("Catalog already contains {} products; skipping seed", existing);
            return;
        }
        int products = Math.max(properties.getProducts(), 0);
        int reviews = Math.max(properties.getReviews(), 0);
        if (products == 0) {
            detail = "empty";
            log.warn("SEED_PRODUCTS is 0; origin will serve an empty catalog");
            return;
        }
        long started = System.nanoTime();
        log.info("Seeding {} products and {} reviews", products, reviews);
        jdbc.update(
                """
                INSERT INTO products (id, name, description, price, category, created_at)
                SELECT g,
                       'Product ' || g,
                       'Description for product ' || g,
                       CAST(round(CAST(1 + random() * 500 AS numeric), 2) AS numeric(12, 2)),
                       (ARRAY['electronics','books','home','sports','toys'])[1 + (g % 5)],
                       NOW()
                FROM generate_series(1, ?) AS g
                """,
                products
        );
        int inserted = 0;
        while (inserted < reviews) {
            int batch = Math.min(REVIEW_BATCH, reviews - inserted);
            jdbc.update(
                    """
                    INSERT INTO reviews (product_id, user_id, rating, comment, created_at)
                    SELECT CAST(1 + floor(random() * ?) AS bigint),
                           CAST(1 + floor(random() * 100000) AS bigint),
                           CAST(1 + floor(random() * 5) AS int),
                           'Seed review',
                           NOW() - (random() * INTERVAL '180 days')
                    FROM generate_series(1, ?)
                    """,
                    products,
                    batch
            );
            inserted += batch;
            log.info("Seeded reviews {}/{}", inserted, reviews);
        }
        jdbc.execute("SELECT setval(pg_get_serial_sequence('reviews','id'), COALESCE((SELECT MAX(id) FROM reviews), 1))");
        jdbc.execute("ANALYZE products");
        jdbc.execute("ANALYZE reviews");
        productCount = countProducts();
        detail = "seeded";
        log.info("Seed complete in {} ms (products={})", (System.nanoTime() - started) / 1_000_000, productCount);
    }

    public boolean isComplete() {
        return complete;
    }

    public String detail() {
        return detail;
    }

    public long productCount() {
        return productCount;
    }

    private long countProducts() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM products", Long.class);
        return count == null ? 0 : count;
    }
}
