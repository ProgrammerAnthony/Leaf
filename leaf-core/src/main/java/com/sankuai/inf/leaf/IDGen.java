package com.sankuai.inf.leaf;

import com.sankuai.inf.leaf.common.Result;

/**
 * 该接口的实现类有三个，分别是号段模式、snowflake以及默认一直返回0的生成器。
 */
public interface IDGen {
    /**
     * 获取指定key下一个id
     * @param key
     * @return
     */
    Result get(String key);

    /**
     * 初始化
     * @return
     */
    boolean init();
}
