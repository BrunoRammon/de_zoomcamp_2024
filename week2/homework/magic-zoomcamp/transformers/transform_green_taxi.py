import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def _to_snake_case(camel_case_str: str)-> str:
    pre_treat = camel_case_str.replace('ID','Id').replace('PU', 'Pu').replace('DO','Do')
    snake_case = re.sub(r'(?<!^)(?=[A-Z])', '_', pre_treat).lower()
    return snake_case

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    rename_map = {col: _to_snake_case(col) for col in data.columns}
    data_treat = ( 
        data[(data.trip_distance != 0) & (data.passenger_count != 0)]
        .assign(
            lpep_pickup_date = lambda df: df.lpep_pickup_datetime.dt.date
        )
        .rename(columns=rename_map)
    )

    return data_treat


@test
def test_column_names(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert "vendor_id" in output.columns, 'The column names must be in snake case'

@test
def test_passenger_count(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output.passenger_count==0).sum() == 0, 'The passenger_count must be different from zero'

@test
def test_trip_distance(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert (output.trip_distance==0).sum() == 0, 'The trip_distance must be different from zero'