UDT_BIN_OP(ADD, +, operator +)
UDT_BIN_OP(MUL, *, operator *)
UDT_BIN_OP(DIV, /, operator /)
UDT_BIN_OP(SUB, -, operator -)

UDT_UNARY_OP(NEG, -, operator -)
UDT_UNARY_OP(SQRT, sqrt, sqrt)

UDT_CMP_OP(LT, <, operator <)
UDT_CMP_OP(GT, >, operator >)
UDT_CMP_OP(LE, <=, operator <=)
UDT_CMP_OP(GE, >=, operator >=)
UDT_CMP_OP(EQ, =, operator ==)
UDT_CMP_OP(NE, <>, operator !=)

UDT_CNV(DOUBLE, Double, double)
UDT_CNV(INT64, Int64, int64_t)
UDT_CNV(STRING, String, char const*)

#undef UDT_BIN_OP
#undef UDT_UNARY_OP
#undef UDT_CMP_OP
#undef UDT_CNV