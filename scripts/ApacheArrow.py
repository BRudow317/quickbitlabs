# pyright: reportAttributeAccessIssue=false
import pyarrow as pa
import pyarrow.compute as pc

# This prints every vectorized function Arrow currently supports
print(pc.list_functions())

# 1. Create an Arrow Array (the "drawer" of data)
numbers = pa.array([10, 20, 30, 40, None, 60])

# 2. Use a built-in compute function
# This is MUCH faster than a Python loop for large datasets
result = pc.multiply(numbers, 2)

print(f"Original: {numbers}")
print(f"Doubled:  {result}")

# 3. Handling Nulls (Arrow is smart about missing data)
mean_val = pc.mean(numbers)
print(f"Mean (ignoring nulls): {mean_val}")

# Create a Table
data = {
    "user_id": [1, 2, 3, 4],
    "score": [88, 92, 75, 95],
    "active": [True, False, True, True]
}
table = pa.table(data)

# Use a function to filter the table
# We want only 'active' users with a 'score' > 80
mask = pc.and_(
    pc.equal(table["active"], True),
    pc.greater(table["score"], 80)
)

filtered_table = table.filter(mask)
print(filtered_table.to_pydict())


# Define a simple logic: Check if a score is 'Elite'
def identify_elite(ctx, scores):
    # Returns a boolean array
    return pc.greater(scores, 90)

# Register it so Arrow knows how to use it
func_name = "is_elite"
pc.register_scalar_function(
    identify_elite, 
    func_name, 
    {"summary": "Finds elite scores"}, 
    {"scores": pa.int64()}, 
    pa.bool_()
)

# Call your custom function through the compute module
is_elite_results = pc.call_function("is_elite", [table["score"]])
print(f"Elite Status: {is_elite_results}")