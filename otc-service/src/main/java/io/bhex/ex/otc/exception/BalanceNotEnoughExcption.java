package io.bhex.ex.otc.exception;

/**
 * @ProjectName:
 * @Package:
 * @Author: yuehao  <hao.yue@bhex.com>
 * @CreateDate: 2020-05-11 19:14
 * @Copyright（C）: 2018 BHEX Inc. All rights reserved.
 */
public class BalanceNotEnoughExcption extends RuntimeException {

    public BalanceNotEnoughExcption(String message) {
        super(message);
    }

}
