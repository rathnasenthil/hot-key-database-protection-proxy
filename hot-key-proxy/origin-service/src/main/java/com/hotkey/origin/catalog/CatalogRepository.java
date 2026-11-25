package com.hotkey.origin.catalog;

import com.hotkey.origin.api.ProductResponse;
import com.hotkey.origin.api.ProductUpdateRequest;
import com.hotkey.origin.api.ReviewCreateRequest;
import com.hotkey.origin.api.ReviewResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class CatalogRepository {

    private final JdbcTemplate jdbc;

    public CatalogRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ProductResponse> findProduct(long id) {
        List<ProductResponse> rows = jdbc.query(
                """
                SELECT id, name, description, price, category, created_at
                FROM products
                WHERE id = ?
                """,
                (rs, row) -> new ProductResponse(
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getString("description"),
                        rs.getBigDecimal("price"),
                        rs.getString("category"),
                        rs.getTimestamp("created_at").toInstant()
                ),
                id
        );
        return rows.stream().findFirst();
    }

    public List<ReviewResponse> findReviews(long productId) {
        return jdbc.query(
                """
                SELECT id, product_id, user_id, rating, comment, created_at
                FROM reviews
                WHERE product_id = ?
                ORDER BY created_at DESC
                LIMIT 20
                """,
                (rs, row) -> new ReviewResponse(
                        rs.getLong("id"),
                        rs.getLong("product_id"),
                        rs.getLong("user_id"),
                        rs.getInt("rating"),
                        rs.getString("comment"),
                        rs.getTimestamp("created_at").toInstant()
                ),
                productId
        );
    }

    public int updateProduct(long id, ProductUpdateRequest request, ProductResponse current) {
        String name = request.name() == null ? current.name() : request.name();
        String description = request.description() == null ? current.description() : request.description();
        var price = request.price() == null ? current.price() : request.price();
        String category = request.category() == null ? current.category() : request.category();
        return jdbc.update(
                """
                UPDATE products
                SET name = ?, description = ?, price = ?, category = ?
                WHERE id = ?
                """,
                name, description, price, category, id
        );
    }

    public ReviewResponse insertReview(long productId, ReviewCreateRequest request) {
        KeyHolder keys = new GeneratedKeyHolder();
        Instant now = Instant.now();
        jdbc.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    """
                    INSERT INTO reviews (product_id, user_id, rating, comment, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    new String[]{"id"}
            );
            statement.setLong(1, productId);
            statement.setLong(2, request.userId());
            statement.setInt(3, request.rating());
            statement.setString(4, request.comment());
            statement.setTimestamp(5, Timestamp.from(now));
            return statement;
        }, keys);
        Number id = keys.getKey();
        if (id == null) {
            throw new IllegalStateException("Review insert did not return an id");
        }
        return new ReviewResponse(id.longValue(), productId, request.userId(), request.rating(), request.comment(), now);
    }

    public boolean productExists(long id) {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM products WHERE id = ?", Integer.class, id);
        return count != null && count > 0;
    }

    public long productCount() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM products", Long.class);
        return count == null ? 0 : count;
    }
}
