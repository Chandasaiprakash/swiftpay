package com.swiftpay.ledger.repository;

import com.swiftpay.ledger.entity.AccountEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface AccountRepository
        extends JpaRepository<AccountEntity, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
        SELECT a
        FROM AccountEntity a
        WHERE a.userId IN :userIds
        ORDER BY a.userId ASC
    """)
    List<AccountEntity> lockAccounts(
            @Param("userIds") List<Long> userIds
    );
}