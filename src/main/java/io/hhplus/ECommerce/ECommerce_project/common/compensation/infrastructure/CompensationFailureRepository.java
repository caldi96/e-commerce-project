package io.hhplus.ECommerce.ECommerce_project.common.compensation.infrastructure;

import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.entity.CompensationFailure;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.enums.CompensationFailureStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CompensationFailureRepository extends JpaRepository<CompensationFailure, Long> {
    List<CompensationFailure> findByStatus(CompensationFailureStatus status);

    List<CompensationFailure> findByStatusOrderByCreatedAtDesc(CompensationFailureStatus status);

    List<CompensationFailure> findByProductId(Long productId);
}
