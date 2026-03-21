package io.hhplus.ECommerce.ECommerce_project.common.compensation.presentation;

import io.hhplus.ECommerce.ECommerce_project.common.compensation.application.CompensationFailureService;
import io.hhplus.ECommerce.ECommerce_project.common.compensation.domain.entity.CompensationFailure;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 보상 트랜잭션 실패 관리 API
 * - 관리자 전용
 */
@RestController
@RequestMapping("/admin/compensation-failures")
@RequiredArgsConstructor
public class CompensationFailureController {

    private final CompensationFailureService compensationFailureService;

    /**
     * 대기 중인 실패 목록 조회
     */
    @GetMapping("/pending")
    public ResponseEntity<List<CompensationFailure>> getPendingFailures() {
        List<CompensationFailure> failures = compensationFailureService.findPendingFailures();
        return ResponseEntity.ok(failures);
    }

    /**
     * 수동 처리 필요한 실패 목록 조회
     */
    @GetMapping("/manual-required")
    public ResponseEntity<List<CompensationFailure>> getManualRequiredFailures() {
        List<CompensationFailure> failures = compensationFailureService.findManualRequiredFailures();
        return ResponseEntity.ok(failures);
    }

    /**
     * 특정 실패 재시도
     */
    @PostMapping("/{id}/retry")
    public ResponseEntity<Map<String, String>> retryCompensation(@PathVariable Long id) {
        try {
            compensationFailureService.manualRetry(id);
            return ResponseEntity.ok(Map.of("message", "재시도 성공"));
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * 수동 해결 처리
     */
    @PostMapping("/{id}/resolve")
    public ResponseEntity<Map<String, String>> resolveCompensation(
            @PathVariable Long id,
            @RequestBody ResolveRequest request
    ) {
        compensationFailureService.markResolved(id, request.resolvedBy(), request.note());
        return ResponseEntity.ok(Map.of("message", "해결 완료"));
    }

    public record ResolveRequest(String resolvedBy, String note) {}
}
