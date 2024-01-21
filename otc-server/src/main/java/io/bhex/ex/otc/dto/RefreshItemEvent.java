package io.bhex.ex.otc.dto;

import lombok.Data;

@Data
public class RefreshItemEvent {

    private Long exchangeId;
    private String tokenId;
    private String currencyId;
}
