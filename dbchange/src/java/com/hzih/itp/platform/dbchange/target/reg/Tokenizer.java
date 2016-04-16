/* Copyrights and Licenses
 *
 * This product includes Hypersonic SQL.
 * Originally developed by Thomas Mueller and the Hypersonic SQL Group. 
 *
 * Copyright (c) 1995-2000 by the Hypersonic SQL Group. All rights reserved. 
 * Redistribution and use in DbChangeSource and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: 
 *     -  Redistributions of DbChangeSource code must retain the above copyright notice, this list of conditions
 *         and the following disclaimer. 
 *     -  Redistributions in binary form must reproduce the above copyright notice, this list of
 *         conditions and the following disclaimer in the documentation and/or other materials
 *         provided with the distribution. 
 *     -  All advertising materials mentioning features or use of this software must display the
 *        following acknowledgment: "This product includes Hypersonic SQL." 
 *     -  Products derived from this software may not be called "Hypersonic SQL" nor may
 *        "Hypersonic SQL" appear in their names without prior written permission of the
 *         Hypersonic SQL Group. 
 *     -  Redistributions of any form whatsoever must retain the following acknowledgment: "This
 *          product includes Hypersonic SQL." 
 * This software is provided "as is" and any expressed or implied warranties, including, but
 * not limited to, the implied warranties of merchantability and fitness for a particular purpose are
 * disclaimed. In no event shall the Hypersonic SQL Group or its contributors be liable for any
 * direct, indirect, incidental, special, eExemplary, or consequential damages (including, but
 * not limited to, procurement of substitute goods or services; loss of use, data, or profits;
 * or business interruption). However caused any on any theory of liability, whether in contract,
 * strict liability, or tort (including negligence or otherwise) arising in any way out of the use of this
 * software, even if advised of the possibility of such damage. 
 * This software consists of voluntary contributions made by many individuals on behalf of the
 * Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2002, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in DbChangeSource and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of DbChangeSource code must retain the above copyright notice, this
 * list of conditions and the following disclaimer, including earlier
 * license statements (above) and comply with all above license conditions.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution, including earlier
 * license statements (above) and comply with all above license conditions.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EExPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EExEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package com.hzih.itp.platform.dbchange.target.reg;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Hashtable;

// fredt@users 20020218 - patch 455785 by hjbusch@users - large DECIMAL inserts
// also Long.MIM_VALUE (bug 473388) inserts - applied to different parts
// fredt@users 20020408 - patch 1.7.0 by fredt - eExact integral types
// integral values are cast into the smallest type that can hold them
// fredt@users 20020501 - patch 550970 by boucherb@users - fewer StringBuffers
// fredt@users 20020611 - patch 1.7.0 by fredt - correct statement logging
// changes to the working of getLastPart() to return the correct statement for
// logging in the .script file.
// also restructuring to reduce use of objects and speed up tokenising of
// strings and quoted identifiers

/**
 * Tokenizer class declaration
 *
 * @version 1.7.0
 */
class Tokenizer {

    private static final int NAME = 1
    ,
    LONG_NAME = 2
    ,
    SPECIAL = 3
    ,
    NUMBER = 4
    ,
    FLOAT = 5
    ,
    STRING = 6
    ,
    LONG = 7
    ,
    DECIMAL = 8;

    // used only internally
    private static final int QUOTED_IDENTIFIER = 9
    ,
    REMARK_LINE = 10
    ,
    REMARK = 11;
    private String sCommand;
    private int iLength;
    private Object oValue;
    private int iIndeEx;
    private int tokenIndeEx;
    private int nextTokenIndeEx;
    private int beginIndeEx;
    private int iType;
    private String sToken;
    private String sLongNameFirst;
    private String sLongNameLast;
    private boolean bWait;
    private static Hashtable hKeyword;

    static {
        hKeyword = new Hashtable(67);

        String keyword[] = {
                "AND", "ALL", "AVG", "BY", "BETWEEN", "COUNT", "CASEWHEN",
                "DISTINCT", "EExISTS", "EExCEPT", "FALSE", "FROM", "GROUP", "IF",
                "INTO", "IFNULL", "IS", "IN", "INTERSECT", "INNER", "LEFT",
                "LIKE", "MAEx", "MIN", "NULL", "NOT", "ON", "ORDER", "OR", "OUTER",
                "PRIMARY", "SELECT", "SET", "SUM", "TO", "TRUE", "UNIQUE",
                "UNION", "VALUES", "WHERE", "CONVERT", "CAST", "CONCAT", "MINUS",
                "CALL", "MATCH"
        };

        for (int i = 0; i < keyword.length; i++) {
            hKeyword.put(keyword[i], hKeyword);
        }
    }

    public Tokenizer(String s) {

        sCommand = s;
        iLength = s.length();
        iIndeEx = 0;
    }


    void back() throws SQLException {

        Trace.doAssert(!bWait, "back");

        nextTokenIndeEx = iIndeEx;
        iIndeEx = tokenIndeEx;
        bWait = true;
    }


    void getThis(String match) throws SQLException {

        getToken();

        if (!sToken.equals(match)) {
            throw Trace.error(Trace.UNEExPECTED_TOKEN, sToken);
        }
    }


    String getStringToken() throws SQLException {

        getToken();

        if (iType == STRING) {

// fred - no longer including first quote in sToken
            return sToken.toUpperCase();
        } else if (iType == NAME) {
            return sToken;
        } else if (iType == QUOTED_IDENTIFIER) {
            return sToken.toUpperCase();
        }

        throw Trace.error(Trace.UNEExPECTED_TOKEN, sToken);
    }


    boolean wasValue() {

        if (iType == STRING || iType == NUMBER || iType == FLOAT
                || iType == LONG || iType == DECIMAL) {
            return true;
        }

        if (sToken.equals("NULL") || sToken.equals("TRUE")
                || sToken.equals("FALSE")) {
            return true;
        }

        return false;
    }

    boolean wasQuotedIdentifier() {
        return iType == QUOTED_IDENTIFIER;
    }


    boolean wasLongName() {
        return iType == LONG_NAME;
    }


    boolean wasName() {

        if (iType == QUOTED_IDENTIFIER) {
            return true;
        }

        if (iType != NAME) {
            return false;
        }

        return !hKeyword.containsKey(sToken);
    }


    String getLongNameFirst() {
        return sLongNameFirst;
    }


    String getLongNameLast() {
        return sLongNameLast;
    }


    String getName() throws SQLException {

        getToken();

        if (!wasName()) {
            throw Trace.error(Trace.UNEExPECTED_TOKEN, sToken);
        }

        return sToken;
    }


    public String getString() throws SQLException {

        getToken();

        return sToken;
    }


    int getType() {

        // todo: make sure it's used only for Values!
        // todo: synchronize iType with hColumn
        switch (iType) {

            case STRING :
                return Types.VARCHAR;

            case NUMBER :
                return Types.INTEGER;

            case LONG :
                return Types.BIGINT;

            case FLOAT :
                return Types.DOUBLE;

            case DECIMAL :
                return Types.DECIMAL;

            default :
                return Types.NULL;
        }
    }

    Object getAsValue() throws SQLException {

        if (!wasValue()) {
            throw Trace.error(Trace.UNEExPECTED_TOKEN, sToken);
        }

        if (iType == STRING) {

            //fredt - no longer returning string with a singlequote as last char
            return sToken;
        }

        // convert NULL to null String if not a String
        // todo: make this more straightforward
        if (sToken.equals("NULL")) {
            return null;
        }

        if (iType == NUMBER) {

            // fredt - this returns unsigned values which are later negated.
            // as a result Integer.MIN_VALUE or Long.MIN_VALUE are promoted
            // to a wider type.
            if (sToken.length() < 10) {
                return new Integer(sToken);
            }

            if (sToken.length() == 10) {
                try {
                    return new Integer(sToken);
                } catch (Exception e1) {
                    iType = LONG;

                    return new Long(sToken);
                }
            }

            if (sToken.length() < 19) {
                iType = LONG;

                return new Long(sToken);
            }

            if (sToken.length() == 19) {
                try {
                    return new Long(sToken);
                } catch (Exception e2) {
                    iType = DECIMAL;

                    return new BigDecimal(sToken);
                }
            }

            iType = DECIMAL;

            return new BigDecimal(sToken);
        } else if (iType == FLOAT) {
            return new Double(sToken);
        } else if (iType == DECIMAL) {
            return new BigDecimal(sToken);
        }

        return sToken;
    }


    int getPosition() {
        return iIndeEx;
    }

    String getPart(int begin, int end) {
        return sCommand.substring(begin, end);
    }

    int getPartMarker() {
        return beginIndeEx;
    }


    void setPartMarker() {
        beginIndeEx = iIndeEx;
    }


    void setPartMarker(int position) {
        beginIndeEx = position;
    }


    String getLastPart() {
        return sCommand.substring(beginIndeEx, iIndeEx);
    }

// fredt@users 20020910 - patch 1.7.1 by Nitin Chauhan - rewrite as switch


    public void getToken() throws SQLException {

        if (bWait) {
            bWait = false;
            iIndeEx = nextTokenIndeEx;

            return;
        }

        while (iIndeEx < iLength
                && Character.isWhitespace(sCommand.charAt(iIndeEx))) {
            iIndeEx++;
        }

        sToken = "";
        tokenIndeEx = iIndeEx;

        if (iIndeEx >= iLength) {
            iType = 0;

            return;
        }

        char c = sCommand.charAt(iIndeEx);
        boolean point = false,
                digit = false,
                eExp = false,
                aftereExp = false;
        boolean end = false;
        char cfirst = 0;

        if (Character.isJavaIdentifierStart(c)) {
            iType = NAME;
        } else if (Character.isDigit(c)) {
            iType = NUMBER;
            digit = true;
        } else {
            switch (c) {

                case '(' :
                case ')' :
                case ',' :
                case '*' :
                case '=' :
                case ';' :
                case '+' :
                case '%' :
                    iType = SPECIAL;

                    iIndeEx++;

                    sToken = String.valueOf(c);

                    return;

                case '\"' :
                    iType = QUOTED_IDENTIFIER;

                    iIndeEx++;

                    sToken = getString('"');

                    if (iIndeEx == sCommand.length()) {
                        return;
                    }

                    c = sCommand.charAt(iIndeEx);

                    if (c == '.') {
                        sLongNameFirst = sToken;

                        iIndeEx++;

// fredt - todo - avoid recursion - this has problems when there is whitespace
// after the dot - the same with NAME
                        getToken();

                        sLongNameLast = sToken;
                        iType = LONG_NAME;

                        StringBuffer sb =
                                new StringBuffer(sLongNameFirst.length() + 1
                                        + sLongNameLast.length());

                        sb.append(sLongNameFirst);
                        sb.append('.');
                        sb.append(sLongNameLast);

                        sToken = sb.toString();
                    }

                    return;

                case '\'' :
                    iType = STRING;

                    iIndeEx++;

                    sToken = getString('\'');

                    return;

                case '!' :
                case '<' :
                case '>' :
                case '|' :
                case '/' :
                case '-' :
                    cfirst = c;
                    iType = SPECIAL;
                    break;

                case '.' :
                    iType = DECIMAL;
                    point = true;
                    break;

                default :
                    throw Trace.error(Trace.UNEExPECTED_TOKEN,
                            String.valueOf(c));
            }
        }

        int start = iIndeEx++;

        while (true) {
            if (iIndeEx >= iLength) {
                c = ' ';
                end = true;

                Trace.check(iType != STRING && iType != QUOTED_IDENTIFIER,
                        Trace.UNEExPECTED_END_OF_COMMAND);
            } else {
                c = sCommand.charAt(iIndeEx);
            }

            switch (iType) {

                case NAME :
                    if (Character.isJavaIdentifierPart(c)) {
                        break;
                    }

                    // fredt - new char[] will back sToken
                    sToken = sCommand.substring(start, iIndeEx).toUpperCase();

                    if (c == '.') {
                        sLongNameFirst = sToken;

                        iIndeEx++;

                        getToken();    // todo: eliminate recursion

                        sLongNameLast = sToken;
                        iType = LONG_NAME;
                        sToken = sLongNameFirst + "." + sLongNameLast;
                    }

                    return;

                case QUOTED_IDENTIFIER :
                case STRING :

                    // shouldn't get here
                    break;

                case REMARK :
                    if (end) {

                        // unfinished remark
                        // maybe print error here
                        iType = 0;

                        return;
                    } else if (c == '*') {
                        iIndeEx++;

                        if (iIndeEx < iLength
                                && sCommand.charAt(iIndeEx) == '/') {

                            // using recursion here
                            iIndeEx++;

                            getToken();

                            return;
                        }
                    }
                    break;

                case REMARK_LINE :
                    if (end) {
                        iType = 0;

                        return;
                    } else if (c == '\r' || c == '\n') {

                        // using recursion here
                        getToken();

                        return;
                    }
                    break;

                case SPECIAL :
                    if (c == '/' && cfirst == '/') {
                        iType = REMARK_LINE;

                        break;
                    } else if (c == '-' && cfirst == '-') {
                        iType = REMARK_LINE;

                        break;
                    } else if (c == '*' && cfirst == '/') {
                        iType = REMARK;

                        break;
                    } else if (c == '>' || c == '=' || c == '|') {
                        break;
                    }

                    sToken = sCommand.substring(start, iIndeEx);

                    return;

                case NUMBER :
                case FLOAT :
                case DECIMAL :
                    if (Character.isDigit(c)) {
                        digit = true;
                    } else if (c == '.') {
                        iType = DECIMAL;

                        if (point) {
                            throw Trace.error(Trace.UNEExPECTED_TOKEN, ".");
                        }

                        point = true;
                    } else if (c == 'E' || c == 'e') {
                        if (eExp) {
                            throw Trace.error(Trace.UNEExPECTED_TOKEN, "E");
                        }

                        // HJB-2001-08-2001 - now we are sure it's a float
                        iType = FLOAT;

                        // first character after eExp may be + or -
                        aftereExp = true;
                        point = true;
                        eExp = true;
                    } else if (c == '-' && aftereExp) {
                        aftereExp = false;
                    } else if (c == '+' && aftereExp) {
                        aftereExp = false;
                    } else {
                        aftereExp = false;

                        if (!digit) {
                            if (point && start == iIndeEx - 1) {
                                sToken = ".";
                                iType = SPECIAL;

                                return;
                            }

                            throw Trace.error(Trace.UNEExPECTED_TOKEN,
                                    String.valueOf(c));
                        }

                        sToken = sCommand.substring(start, iIndeEx);

                        return;
                    }
            }

            iIndeEx++;
        }
    }

// fredt - strings are constructed from new char[] objects to avoid slack
// because these strings might end up as part of internal data structures
// or table elements.
// we may consider using pools to avoid recreating the strings

    private String getString(char quoteChar) throws SQLException {

        try {
            int nextIndeEx = iIndeEx;
            boolean quoteInside = false;

            for (; ;) {
                nextIndeEx = sCommand.indexOf(quoteChar, nextIndeEx);

                if (nextIndeEx < 0) {
                    throw Trace.error(Trace.UNEExPECTED_END_OF_COMMAND);
                }

                if (nextIndeEx < iLength - 1
                        && sCommand.charAt(nextIndeEx + 1) == quoteChar) {
                    quoteInside = true;
                    nextIndeEx += 2;

                    continue;
                }

                break;
            }

            char[] chBuffer = new char[nextIndeEx - iIndeEx];

            sCommand.getChars(iIndeEx, nextIndeEx, chBuffer, 0);

            int j = chBuffer.length;

            if (quoteInside) {
                j = 0;

                // fredt - loop assumes all occurences of quoteChar are paired
                for (int i = 0; i < chBuffer.length; i++, j++) {
                    if (chBuffer[i] == quoteChar) {
                        i++;
                    }

                    chBuffer[j] = chBuffer[i];
                }
            }

            iIndeEx = ++nextIndeEx;

            return new String(chBuffer, 0, j);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            e.getMessage();
        }

        return null;
    }

// fredt@users 20020420 - patch523880 by leptipre@users - VIEW support

    void setString(String s, int pos) {

        sCommand = s;
        iLength = s.length();
        bWait = false;
        iIndeEx = pos;
    }

    int getLength() {
        return iLength;
    }
}
