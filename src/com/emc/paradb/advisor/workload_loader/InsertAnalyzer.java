package com.emc.paradb.advisor.workload_loader;

import java.util.List;

import com.emc.paradb.advisor.data_loader.DataLoader;
import com.emc.paradb.advisor.data_loader.TableAttributes;
import com.emc.paradb.advisor.data_loader.TableNode;

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
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;


/**
 * translate an insert sql statement into a in-memory object we define
 * it is based on the visitor design pattern
 * 
 * notice that not all kinds of sql statement are supported.
 * currenty equal statement for int, string are well tested
 * however, range statement, etc are not well-tested.
 * 
 * @author xin pan
 *
 */

public class InsertAnalyzer {
	
	public InsertAnalyzer(){}
	
	public InsertAnalysisInfo analyze(Insert insert){
		
		InsertAnalysisInfo info = new InsertAnalysisInfo();
		
		info.setTable(insert.getTable().getName());
		
		List<Column> columns = insert.getColumns();
		if(columns != null)
			for (Column col: columns)
				info.addKey(col.getColumnName());
		else
		{
			TableNode tableNode = DataLoader.getDBData().getMetaData().get(info.getTable());
			
			for(int i = 0; i < tableNode.getAttrVector().size(); i++)
				info.addKey(tableNode.getAttrVector().get(i).getName());
		}
		
		insert.getItemsList().accept(new InsertItemsListVisitor(info));
		
		info.finalize();
		
		return info;
	}

}

class InsertItemsListVisitor implements ItemsListVisitor{
	
	InsertAnalysisInfo info;
	
	public InsertItemsListVisitor(InsertAnalysisInfo info){
		this.info = info;
	}	

	@Override
	public void visit(SubSelect arg0) {
	}

	@Override
	public void visit(ExpressionList arg0) {
		
		InsertExpressionVisitor exprVisitor = new InsertExpressionVisitor(info);
		
		List<Expression> exprs = arg0.getExpressions();
		for (Expression expr: exprs){
			expr.accept(exprVisitor);
		}
	}
	
}

class InsertExpressionVisitor implements ExpressionVisitor {
	
	InsertAnalysisInfo info;
	
	public InsertExpressionVisitor(InsertAnalysisInfo info){
		this.info = info;
	}
	
	public void addConst(String value){
		info.addValue(value);
	}

	@Override
	public void visit(DoubleValue arg0) {
		addConst(arg0.toString());
	}

	@Override
	public void visit(LongValue arg0) {
		addConst(arg0.toString());
	}

	@Override
	public void visit(DateValue arg0) {
		addConst(arg0.toString());
	}

	@Override
	public void visit(TimeValue arg0) {
		addConst(arg0.toString());
	}

	@Override
	public void visit(TimestampValue arg0) {
		addConst(arg0.toString());
	}

	@Override
	public void visit(StringValue arg0) {
		addConst(arg0.toString());
	}

	@Override
	public void visit(AndExpression arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
	}

	@Override
	public void visit(EqualsTo arg0) 
	{
		System.out.println("No support for " + arg0.getStringExpression());
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
	{	}

	@Override
	public void visit(NullValue arg0) 
	{
		addConst(arg0.toString());
	}

	@Override
	public void visit(Function arg0) { }

	@Override
	public void visit(InverseExpression arg0) {	}

	@Override
	public void visit(JdbcParameter arg0) {	}

	@Override
	public void visit(Parenthesis arg0) 
	{
		Expression pe = arg0.getExpression();
		pe.accept(this);
	}

	@Override
	public void visit(Addition arg0) {	}

	@Override
	public void visit(Division arg0) {	}

	@Override
	public void visit(Multiplication arg0) {	}

	@Override
	public void visit(Subtraction arg0) {	}

	@Override
	public void visit(OrExpression arg0) {	}

	@Override
	public void visit(Between arg0) {	}

}