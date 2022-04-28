package com.atguigu.gmallpublisher.controller;

import com.atguigu.gmallpublisher.service.GmvService;
import org.apache.ibatis.annotations.Select;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

//@Controller
@RestController // = @Controller+@ResponseBody
@RequestMapping("/api/sugar")
public class SugarController {

    @Autowired
    private GmvService gmvService;

    @RequestMapping("/test")
    //@ResponseBody
    public String test1() {
        System.out.println("aaaaaaa");
        return "{\"id\":\"1001\",\"name\":\"zhangsan\"}";
    }

    @RequestMapping("/test2")
    public String test2(@RequestParam("nn") String name,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(name + ":" + age);
        return "success";
    }

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        if (date == 0) {
            date = getToday();
        }

        //查询数据
        Double gmv = gmvService.getGmv(date);

        //拼接并返回结果数据
        return "{ " +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": " + gmv +
                "}";
    }

    private int getToday() {
        long ts = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return Integer.parseInt(sdf.format(ts));
    }


}
