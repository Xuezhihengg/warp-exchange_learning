package org.warpexchange_learning.common.model.ui;


import jakarta.persistence.*;
import org.warpexchange_learning.common.enums.UserType;
import org.warpexchange_learning.common.model.support.EntitySupport;

@Entity
@Table(name = "users")
public class UserEntity implements EntitySupport {

    /**
     * Primary key: auto-increment long.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(nullable = false, updatable = false)
    public Long id;

    @Column(nullable = false, updatable = false, length = VAR_ENUM)
    public UserType type;

    /**
     * Created time (milliseconds).
     */
    @Column(nullable = false, updatable = false)
    public long createdAt;

    @Override
    public String toString() {
        return "UserEntity [id=" + id + ", type=" + type + ", createdAt=" + createdAt + "]";
    }
}
