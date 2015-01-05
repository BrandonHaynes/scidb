<%@ page import="java.sql.*" %>
<%@ page import="java.math.BigDecimal" %>
<html>
<head>
<style>
#layout
{
    text-align: center;
}
#body
{
    text-align: left;
    width: 1000px;
    display: block;
    margin-left: auto;
    margin-right: auto;
    padding: 10px;
}
#error
{
    color: red;
}
#queryInput
{
    width: 300px;
}
table
{
    border-collapse: collapse;
}
td,th
{
    border: solid 1px black;
    text-align: center;
    padding: 0px 3px 0px 3px;
}
</style>
</head>
<body>
<div id="layout">
<div id="body">
Enter your AQL query:
<form method="POST">
<input id="queryInput" name="query">
<input type="submit">
</form>
<%
try
{   
    Class.forName("org.scidb.jdbc.Driver");
    if(request.getParameter("query") != null)
    {
        String queryString = request.getParameter("query");
        Connection conn = DriverManager.getConnection("jdbc:scidb://localhost/");
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery(queryString);
        if (res != null)
        {
            ResultSetMetaData meta = res.getMetaData();
            out.print("<table>");
            out.print("<tr>");
            for (int i = 1; i <= meta.getColumnCount(); i++)
            {
                out.print("<th>");
                out.print(meta.getColumnName(i) + "(" + meta.getColumnTypeName(i) + ")");
                out.print("</th>");
            }
            out.print("</tr>");
            while(!res.isAfterLast())
            {
                out.print("<tr>");
                for (int i = 1; i <= meta.getColumnCount(); i++)
                {
                    String t = meta.getColumnTypeName(i);
                    out.print("<td>");
                    if (t.equals("int8") || t.equals("uint8") || t.equals("int16") || t.equals("uint16") || t.equals("int32") || t.equals("uint32") || t.equals("int64"))
                    {
                        long val = res.getLong(i);
                        out.println(res.wasNull() ? "NULL" : val);
                    }
                    else if (t.equals("uint64"))
                    {
                        BigDecimal val = res.getBigDecimal(i);
                        out.println(res.wasNull() ? "NULL" : val);
                    }
                    else if (t.equals("float") || t.equals("double"))
                    {
                        double val = res.getDouble(i);
                        out.println(res.wasNull() ? "NULL" : val);
                    }
                    else if (t.equals("bool"))
                    {
                        boolean val = res.getBoolean(i);
                        out.println(res.wasNull() ? "NULL" : val);
                    }
                    else if (t.equals("string") || t.equals("char"))
                    {
                        String val = res.getString(i);
                        out.println(res.wasNull() ? "NULL" : "'" + val + "'");
                    }
                    else
                    {
                        out.println(String.format("Type %s not supported", t));
                    }
                    out.print("</td>");
                }
                out.print("<tr>");
                res.next();
            }
            out.print("</table>");
            conn.commit();
        }
        else
        {
            out.print("Query was executed");
        }
        conn.close();
    }
}
catch (Exception e)
{
 out.print("Error: <pre id='error'>" + e + "</pre>");
}
%>
</div>
</div>
</body>
</html>
