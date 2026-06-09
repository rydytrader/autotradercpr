package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * One options-strategy instance the operator has created. Backs both short straddles AND
 * short strangles via the {@link #type} discriminator column ({@code STRADDLE} or
 * {@code STRANGLE}); the runtime materialises each row into the right Java object
 * ({@code ShortStraddle} or {@code ShortStrangle}) via its dedicated InstanceManager.
 *
 * <p>Each row gets its own settings under {@code strategies.inst-<id>.*}, state file
 * ({@code short-<type>-inst-<id>-state.json}), sidebar entry and dashboard at
 * {@code /strategies/inst-<id>}.
 *
 * <p>{@code shortCode} is the human-friendly label shown in the sidebar (e.g. {@code 9:20}).
 * {@code active = false} is a soft-delete marker — sessions / trades / settings / state file
 * are preserved; the instance is hidden from the registry until manually restored.
 */
@Entity
@Table(name = "strategy_instances",
       uniqueConstraints = @UniqueConstraint(name = "uk_strategy_instance_short_code",
                                             columnNames = "short_code"))
public class StrategyInstanceEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 80)
    private String name;

    @Column(length = 240)
    private String description;

    @Column(name = "short_code", nullable = false, length = 24)
    private String shortCode;

    @Column(nullable = false)
    private boolean enabled = true;

    @Column(nullable = false)
    private boolean active = true;

    /** Discriminator — {@code STRADDLE} (default for backwards compat) or {@code STRANGLE}.
     *  Pre-existing rows get backfilled to {@code STRADDLE} by {@code SchemaMigration} on
     *  first boot after deploy. */
    @Column(name = "strategy_type", nullable = false, length = 20)
    private String type = "STRADDLE";

    @Column(name = "created_at", nullable = false)
    private long createdAtMillis;

    @Column(name = "updated_at", nullable = false)
    private long updatedAtMillis;

    public StrategyInstanceEntity() {}

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getShortCode() { return shortCode; }
    public void setShortCode(String shortCode) { this.shortCode = shortCode; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public long getCreatedAtMillis() { return createdAtMillis; }
    public void setCreatedAtMillis(long createdAtMillis) { this.createdAtMillis = createdAtMillis; }
    public long getUpdatedAtMillis() { return updatedAtMillis; }
    public void setUpdatedAtMillis(long updatedAtMillis) { this.updatedAtMillis = updatedAtMillis; }

    /** Canonical strategy id used by the rest of the system: {@code inst-<id>}. Stable across
     *  renames of name / description / shortCode. Same scheme for both straddles and strangles
     *  — the {@link #type} discriminator says which kind. */
    public String strategyId() {
        return id == null ? null : ("inst-" + id);
    }
}
