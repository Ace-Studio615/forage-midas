package com.jpmc.midascore.foundation;

import java.math.BigDecimal;

public class Incentive {
    private BigDecimal amount;

    public Incentive(){}

    public Incentive(BigDecimal amount){
        this.amount = amount;
    }

    public BigDecimal getAmount() {
        return amount;
    }
    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
}
