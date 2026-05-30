package com.rydytrader.autotrader.entity;

import jakarta.persistence.*;

/**
 * One short-straddle instance the operator has created. Each row materialises at runtime into
 * a {@code ShortStraddle} object with its own settings ({@code strategies.inst-<id>.*}), state
 * file ({@code short-straddle-inst-<id>-state.json}), sidebar entry and dashboard at
 * {@code /strategies/inst-<id>}.
 *
 * <p>{@code shortCode} is the human-friendly label shown in the sidebar (e.g. {@code 9:20}).
 * {@code active = false} is a soft-delete marker — sessions / trades / settings / state file
 * are preserved; the instance is hidden from the registry until manually restored.
 */
@Entity
@Table(name = "straddle_instances",
       uniqueConstraints = @UniqueConstraint(name = "uk_straddle_instance_short_code",
                                             columnNames = "short_code"))
public class StraddleInstanceEntity {

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

    @Column(name = "created_at", nullable = false)
    private long createdAtMillis;

    @Column(name = "updated_at", nullable = false)
    private long updatedAtMillis;

    public StraddleInstanceEntity() {}

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
    public long getCreatedAtMillis() { return createdAtMillis; }
    public void setCreatedAtMillis(long createdAtMillis) { this.createdAtMillis = createdAtMillis; }
    public long getUpdatedAtMillis() { return updatedAtMillis; }
    public void setUpdatedAtMillis(long updatedAtMillis) { this.updatedAtMillis = updatedAtMillis; }

    /** Canonical strategy id used by the rest of the system: {@code inst-<id>}. Stable across
     *  renames of name / description / shortCode. */
    public String strategyId() {
        return id == null ? null : ("inst-" + id);
    }
}
