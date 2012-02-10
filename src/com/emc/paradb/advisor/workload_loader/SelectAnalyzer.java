package com.emc.paradb.advisor.workload_loader;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class SelectAnalyzer implements SelectVisitor 
{
	SelectAnalysisInfo info;

	public SelectAnalyzer() {	}

	public SelectAnalysisInfo analyze(Select select) 
	{
		info = new SelectAnalysisInfo();
		select.getSelectBody().accept(this);
		return info;
	}

	@Override
	public void visit(PlainSelect arg0) 
	{
		arg0.getFromItem().accept(new SelectFromItemVisitor(info));
	
		List<Join> l = arg0.getJoins();
		if(l != null)
			for(Join join : l)
			{
				FromItem fromItem = join.getRightItem();
				fromItem.accept(new SelectFromItemVisitor(info));
			}

		if (arg0.getWhere() != null){
			arg0.getWhere().accept(new SelectExpressionVisitor(info));
		}
	}

	@Override
	public void visit(Union arg0) {	}

}

class SelectFromItemVisitor implements FromItemVisitor 
{
	SelectAnalysisInfo info;

	public SelectFromItemVisitor(SelectAnalysisInfo info) {
		this.info = info;
	}

	@Override
	public void visit(Table arg0) 
	{
		info.getTables().add(arg0.getName());
	}

	@Override
	public void visit(SubSelect arg0) {}

	@Override
	public void visit(SubJoin arg0) {	}
}

class SelectExpressionVisitor implements ExpressionVisitor {
	
	SelectAnalysisInfo info;
	Set<EqualsTo> hold = new HashSet<EqualsTo>();
	
	public SelectExpressionVisitor(SelectAnalysisInfo info){
		this.info = info;
	}

	@Override
	public void visit(AndExpression arg0) 
	{
		Expression le = arg0.getLeftExpression();
		le.accept(this);
		Expression re = arg0.getRightExpression();
		re.accept(this);
		
		//for those t1.a = t2.b and t1.a = x, we add t1.b = x to WhereKeys
		for(EqualsTo aEqualTo : hold)
		{
			Column cl = (Column)aEqualTo.getLeftExpression();
			Column cr = (Column)aEqualTo.getRightExpression();
			
			WhereKey newWk = null;
			for(WhereKey wk : info.getWhereKeys())
			{	
				if(wk.getKeyName().equals(cl.getColumnName())){
					newWk = new WhereKey();
					newWk.setKeyName(cr.getColumnName());
					newWk.setKeyValue(wk.getKeyValue());
					break;
				}
				else if(wk.getKeyName().equals(cr.getColumnName())){
					newWk = new WhereKey();
					newWk.setKeyName(cl.getColumnName());
					newWk.setKeyValue(wk.getKeyValue());	
					break;
				}			
			}
			if(newWk != null)
				info.getWhereKeys().add(newWk);
			else
				System.out.println("Warning: join A.a = B.b without A.a = x or B.b = y. Ignored");
		}
	}

	@Override
	public void visit(EqualsTo arg0) 
	{
		// equi-joins
		Column column = null;
		Object value = null;
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column) 
		{	
			hold.add(arg0);
			return;
		}
		else if (arg0.getLeftExpression() instanceof Column)
		{			
			column = (Column)arg0.getLeftExpression();
			value = arg0.getRightExpression();
		}
		else if (arg0.getRightExpression() instanceof Column)
		{	
			column = (Column)arg0.getRightExpression();
			value = arg0.getLeftExpression();
		}
		else
		{
			System.out.println("Equal to expression parser error");
			return;
		}
		
		WhereKey wk = new WhereKey();
		wk.setKeyName(column.getColumnName());
		if(value instanceof LongValue)
			wk.setKeyValue(((LongValue)value).getValue());
		else if(value instanceof StringValue)
			wk.setKeyValue(((StringValue)value).getValue());
		
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