package io.bhex.ex.otc.dto;

import lombok.Data;

@Data
public class SymbolDto {

    private Long exchangeId;
    private String tokenId;
    private String currencyId;
}
