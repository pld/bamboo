Notes for DataFrame indices
---------------------------

* make `index` a reserved column name
* specify custom index values by passing a column `index` with your dataset
* save obervations, take column `index` as the index column
  * if no column named `index` call reset_index and take the pandas index

* additions/updates that specify an `index` value must maintain the uniquity of
  the `index` column
* if a custom index is supplied, and an update does not have an index value
  * we would like to assign an index value in a natural way
    * if the index column is numeric, take the highest index value an assign
      the new data `highest index value + 1`
    * do we require a numeric index?
    
Other Notes
-----------

* why is dataset_observations_id different from dataset_id?
