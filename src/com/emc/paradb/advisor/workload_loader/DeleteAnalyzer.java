package com.emc.paradb.advisor.workload_loader;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.SubSelect;

public class DeleteAnalyzer {
	
	DeleteAnalysisInfo info = new DeleteAnalysisInfo();
	
	public DeleteAnalyzer(){
	}

	public DeleteAnalysisInfo analyze(Delete delete){
		
		info.setTable(delete.getTable().getName());
		
		delete.getWhere().accept(new DeleteExpressionVisitor(info));
		
		return info;
	}
}

class DeleteExpressionVisitor implements ExpressionVisitor {
	
	DeleteAnalysisInfo info;
	
	public DeleteExpressionVisitor(DeleteAnalysisInfo info){
		this.info = info;
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(EqualsTo arg0) {
		
		if (arg0.getLeftExpression() instanceof Column && 
				(arg0.getRightExpression() instanceof LongValue)){
			
			Column cl = (Column)arg0.getLeftExpression();
			LongValue lv = (LongValue)arg0.getRightExpression();
			
			WhereKey wk = new WhereKey();
			wk.setTableName(cl.getTable().getName());
			wk.setKeyName(cl.getColumnName());
			wk.setKeyValue(lv.getValue());
			
			info.getWhereKeys().add(wk);
		}
	}

	@Override
	public void visit(GreaterThan arg0) {	}

	@Override
	public void visit(GreaterThanEquals arg0) {	}

	@Override
	public void visit(InExpression arg0) {	}

	@Override
	public void visit(IsNullExpression arg0) {	}

	@Override
	public void visit(LikeExpression arg0) {	}

	@Override
	public void visit(MinorThan arg0) {	}

	@Override
	public void visit(MinorThanEquals arg0) {	}

	@Override
	public void visit(NotEqualsTo arg0) {	}

	@Override
	public void visit(Column arg0) {	}

	@Override
	public void visit(SubSelect arg0) {	}

	@Override
	public void visit(CaseExpression arg0) {	}

	@Override
	public void visit(WhenClause arg0) {	}

	@Override
	public void visit(ExistsExpression arg0) {	}

	@Override
	public void visit(AllComparisonExpression arg0) {	}

	@Override
	public void visit(AnyComparisonExpression arg0) {	}

	@Override
	public void visit(Concat arg0) {	}

	@Override
	public void visit(Matches arg0) {	}

	@Override
	public void visit(BitwiseAnd arg0) {	}

	@Override
	public void visit(BitwiseOr arg0) {	}

	@Override
	public void visit(BitwiseXor arg0) {	}

	@Override
	public void visit(NullValue arg0) {	}

	@Override
	public void visit(Function arg0) { }

	@Override
	public void visit(InverseExpression arg0) {	}

	@Override
	public void visit(JdbcParameter arg0) {	}

	@Override
	public void visit(DoubleValue arg0) {	}

	@Override
	public void visit(LongValue arg0) {	}

	@Override
	public void visit(DateValue arg0) {	}

	@Override
	public void visit(TimeValue arg0) {	}

	@Override
	public void visit(TimestampValue arg0) {	}

	@Override
	public void visit(Parenthesis arg0) {	}

	@Override
	public void visit(StringValue arg0) {	}

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