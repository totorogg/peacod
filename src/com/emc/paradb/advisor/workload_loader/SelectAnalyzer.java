package com.emc.paradb.advisor.workload_loader;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.emc.paradb.advisor.workload_loader.WhereKey.Range;

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
	WhereKey wk = null;
	
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
			
			wk = null;
			for(WhereKey whereKey : info.getWhereKeys())
			{	
				if(whereKey.getKeyName().equals(cl.getColumnName()) && whereKey.getKeyValue() != null){
					wk = new WhereKey();
					wk.setKeyName(cr.getColumnName());
					wk.setKeyValue(whereKey.getKeyValue().toString());
					break;
				}
				else if(whereKey.getKeyName().equals(cr.getColumnName()) && whereKey.getKeyValue() != null){
					wk = new WhereKey();
					wk.setKeyName(cl.getColumnName());
					wk.setKeyValue(whereKey.getKeyValue().toString());	
					break;
				}			
			}
			if(wk != null)
				info.getWhereKeys().add(wk);
			else
			{
				wk = new WhereKey();
				wk.setKeyName(cl.getColumnName());
				wk.setRange(Range.ALL);
				info.getWhereKeys().add(wk);
				
				wk = new WhereKey();
				wk.setKeyName(cr.getColumnName());
				wk.setRange(Range.ALL);
				info.getWhereKeys().add(wk);
			}
		}
	}

	@Override
	public void visit(EqualsTo arg0) 
	{
		wk = new WhereKey();
		wk.setRange(Range.EQUAL);
		if (arg0.getLeftExpression() instanceof Column
				&& arg0.getRightExpression() instanceof Column) 
		{	
			hold.add(arg0);
			return;
		}
		else if (arg0.getLeftExpression() instanceof Column)
		{		
			wk.setKeyName(((Column)arg0.getLeftExpression()).getColumnName());
			arg0.getRightExpression().accept(this);
			info.getWhereKeys().add(wk);	
		}
		else if (arg0.getRightExpression() instanceof Column)
		{	
			wk.setKeyName(((Column)arg0.getRightExpression()).getColumnName());
			arg0.getLeftExpression().accept(this);
			info.getWhereKeys().add(wk);	
		}
		else
		{
			System.out.println("Equal to expression parser error");
			return;
		}
		
	}

	@Override
	public void visit(GreaterThan arg0) 
	{
		wk = new WhereKey();
		wk.setRange(Range.LARGER);
		if(arg0.getLeftExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getLeftExpression()).getColumnName());
			arg0.getRightExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else if(arg0.getRightExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getRightExpression()).getColumnName());
			arg0.getLeftExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else
		{
			System.out.println("Error greater than expression: " + arg0.getStringExpression());
		}
	}

	@Override
	public void visit(GreaterThanEquals arg0) 
	{
		wk = new WhereKey();
		wk.setRange(Range.LARGEEQL);
		if(arg0.getLeftExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getLeftExpression()).getColumnName());
			arg0.getRightExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else if(arg0.getRightExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getRightExpression()).getColumnName());
			arg0.getLeftExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else
		{
			System.out.println("Error >= expression: " + arg0.getStringExpression());
		}
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
		wk = new WhereKey();
		wk.setRange(Range.SMALLER);
		if(arg0.getLeftExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getLeftExpression()).getColumnName());
			arg0.getRightExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else if(arg0.getRightExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getRightExpression()).getColumnName());
			arg0.getLeftExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else
		{
			System.out.println("Error < expression: " + arg0.getStringExpression());
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) 
	{
		wk = new WhereKey();
		wk.setRange(Range.SMALLEQL);
		if(arg0.getLeftExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getLeftExpression()).getColumnName());
			arg0.getRightExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else if(arg0.getRightExpression() instanceof Column)
		{
			wk.setKeyName(((Column)arg0.getRightExpression()).getColumnName());
			arg0.getLeftExpression().accept(this);
			info.getWhereKeys().add(wk);
		}
		else
		{
			System.out.println("Error <= expression: " + arg0.getStringExpression());
		}
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
		wk.setKeyValue(arg0.toString());
	}

	@Override
	public void visit(LongValue arg0) 
	{
		wk.setKeyValue(arg0.toString());
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
		wk.setKeyValue(arg0.toString());
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
		String[] parts = arg0.toString().split("-");
		try
		{
			int first = Integer.valueOf(parts[0].trim());
			int second = Integer.valueOf(parts[1].trim());
			wk.setKeyValue(String.valueOf(first-second));
		}
		catch(NumberFormatException e)
		{
			System.out.println("No support for non integer:" + arg0.getStringExpression());
			return;
		}
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