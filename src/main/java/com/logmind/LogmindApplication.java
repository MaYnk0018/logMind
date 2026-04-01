package main.java.com.logmind;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling   // needed for Phase 2 LogBatcher flush timer
public class LogmindApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogmindApplication.class, args);
    }
}