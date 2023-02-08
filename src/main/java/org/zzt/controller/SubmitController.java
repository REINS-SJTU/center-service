package org.zzt.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zzt.service.SubmitService;

@Slf4j
@RestController
public class SubmitController {
    @Autowired
    SubmitService submitService;

    @GetMapping("/run")
    public void run() throws Exception {
        submitService.run();
    }
}
