package com.example.chatroom;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * 聊天室后端启动类
 */
@SpringBootApplication
@EnableAspectJAutoProxy(exposeProxy = true) // 暴露代理对象，支持同类内事务方法自调用
public class ChatroomApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChatroomApplication.class, args);
    }
}
