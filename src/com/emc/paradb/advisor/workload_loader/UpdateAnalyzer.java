package com.emc.paradb.advisor.workload_loader;



import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;

public class UpdateAnalyzer {
	
	UpdateAnalysisInfo info = new UpdateAnalysisInfo();
	
	public UpdateAnalyzer(){
	}

	public UpdateAnalysisInfo analyze(Update update){
		
		info.setTable(update.getTable().getName());
		info.setSetKeys(update.getColumns(), update.getExpressions());
		update.getWhere().accept(new UpdateExpressionVisitor(info));
		
		return info;
	}
}

class UpdateExpressionVisitor implements ExpressionVisitor {
	
	UpdateAnalysisInfo info;
	
	public UpdateExpressionVisitor(UpdateAnalysisInfo info){
		this.info = info;
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(EqualsTo arg0) 
	{
		Object value = null;
		Column column = null;
		
		if (arg0.getLeftExpression() instanceof Column)
		{	
			column = (Column)arg0.getLeftExpression();
			value = arg0.getRightExpression();			
		}
		else if (arg0.getRightExpression() instanceof Column)
		{
			column = (Column)arg0.getRightExpression();
			value = arg0.getLeftExpression();		
		}
		
		WhereKey wk = new WhereKey();
		wk.setKeyName(column.getColumnName());
		if(value instanceof LongValue)
			wk.setKeyValue(((LongValue)value).getValue());
		else if(value instanceof StringValue)
			wk.setKeyValue(((StringValue)value).getValue());
		else
			try 
			{
				throw new Exception("Unrecognized value type");
			} catch (Exception e) {
				e.printStackTrace();
			}
		
		info.getWhereKeys().add(wk);
	}

	@Override
	public void visit(GreaterThan arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(GreaterThanEquals arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(InExpression arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(IsNullExpression arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(LikeExpression arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(MinorThan arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(MinorThanEquals arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(NotEqualsTo arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(Column arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(SubSelect arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(CaseExpression arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(WhenClause arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(ExistsExpression arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(AllComparisonExpression arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(AnyComparisonExpression arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(Concat arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(Matches arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(BitwiseAnd arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(BitwiseOr arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(BitwiseXor arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(NullValue arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(Function arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(InverseExpression arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(JdbcParameter arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(DoubleValue arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(LongValue arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(DateValue arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(TimeValue arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(TimestampValue arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(Parenthesis arg0) 
	{
		Expression pe = arg0.getExpression();
		pe.accept(this);
	}

	@Override
	public void visit(StringValue arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

	@Override
	public void visit(Addition arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(Division arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(Multiplication arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(Subtraction arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(OrExpression arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(Between arg0) 
	{
		System.out.println("No support for " + arg0.toString());
	}

}