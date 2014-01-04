using CqlSharp.Linq.Expressions;
using System.Linq.Expressions;

namespace CqlSharp.Linq
{
    class SelectBuilder : BuilderBase
    {
        public ProjectionExpression UpdateSelect(ProjectionExpression projection, Expression selectExpression)
        {
            //get the lambda expression of the select method
            var lambda = (LambdaExpression)selectExpression.StripQuotes();

            //map the arguments of the lambda expression to the existing projection
            MapLambdaParameters(lambda, projection.Projection);

            //get the new projections
            var newProjection = Visit(lambda.Body);

            //return existing projection if not changed
            if (newProjection == selectExpression)
                return projection;

            //create new projection
            return new ProjectionExpression(projection.Select, newProjection, projection.ResultFunction);
        }
    }
}
