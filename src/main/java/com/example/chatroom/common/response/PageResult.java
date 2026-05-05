package com.example.chatroom.common.response;

import lombok.Data;

import java.util.List;

/**
 * 游标分页响应封装
 */
@Data
public class PageResult<T> {

    private List<T> list;
    /** 下一页游标（消息ID），null 表示没有更多 */
    private Long nextCursor;
    private boolean hasMore;
    private Long total;

    public static <T> PageResult<T> of(List<T> list, Long nextCursor, boolean hasMore) {
        PageResult<T> r = new PageResult<>();
        r.list = list;
        r.nextCursor = nextCursor;
        r.hasMore = hasMore;
        return r;
    }
}
