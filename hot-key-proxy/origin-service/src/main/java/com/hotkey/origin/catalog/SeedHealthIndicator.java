package com.hotkey.origin.catalog;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class SeedHealthIndicator implements HealthIndicator {

    private final DataSeeder seeder;

    public SeedHealthIndicator(DataSeeder seeder) {
        this.seeder = seeder;
    }

    @Override
    public Health health() {
        if (!seeder.isComplete()) {
            return Health.down()
                    .withDetail("seed", seeder.detail())
                    .build();
        }
        return Health.up()
                .withDetail("seed", seeder.detail())
                .withDetail("products", seeder.productCount())
                .build();
    }
}
