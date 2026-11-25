package com.hotkey.gateway.sim;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

@Controller
public class SimulationController {

    private final WorkloadRunner runner;

    public SimulationController(WorkloadRunner runner) {
        this.runner = runner;
    }

    @GetMapping("/dashboard")
    public String dashboard() {
        return "redirect:/";
    }

    @GetMapping("/api/sim/status")
    @ResponseBody
    public Map<String, Object> status() {
        return runner.status();
    }

    @PostMapping("/api/sim/workloads")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> start(@RequestBody StartRequest request) {
        try {
            return ResponseEntity.accepted().body(runner.start(request));
        } catch (IllegalArgumentException exception) {
            return ResponseEntity.badRequest().body(Map.of("error", exception.getMessage()));
        } catch (IllegalStateException exception) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", exception.getMessage()));
        }
    }

    @PostMapping("/api/sim/stop")
    @ResponseBody
    public Map<String, Object> stop() {
        return runner.stop();
    }

    public record StartRequest(String workload, String mode, Integer durationSeconds, Integer concurrency) {
    }
}
