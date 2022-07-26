-- Returns a table with the most used 5 ingredients in all orders in the past year (6 months could not be possible due to the orders dates) 

SELECT i.id AS ingredient_id, 
(
    SELECT 
    count(*) AS ct 
    FROM Pizza p
    JOIN Orders o ON p.id = o.pizza_id
    WHERE i.id = any(string_to_array(p.ingredients, ',')::int[]) AND CURRENT_DATE - o.order_time <= 365
) + 
(
    SELECT count(*) AS ct 
    FROM Orders o 
    WHERE i.id = any(string_to_array(o.extras, ',')::int[]) 
    AND CURRENT_DATE - o.order_time <= 365 AND o.extras != 'null'
) -
(
    SELECT count(*) AS ct 
    FROM Orders o 
    WHERE i.id = any(string_to_array(o.exclusions, ',')::int[]) 
    AND CURRENT_DATE - o.order_time <= 365 AND o.exclusions != 'null'      
) AS qty, i.name AS ingredient_name
FROM Ingredients i GROUP BY 1 ORDER BY qty DESC LIMIT 5;


-- Creates the recipe for no-extra ingredients
CREATE OR REPLACE FUNCTION printSimpleRecipe (pizza_id int)
RETURNS varchar
LANGUAGE plpgsql
as
$$
declare
	recipe varchar(100);
    ing record;
BEGIN
    for ing in select i.name 
           from Ingredients i
           join Pizza p on i.id = any(string_to_array(p.ingredients, ',')::int[])
           where p.id = pizza_id
    loop
        select into recipe CONCAT(recipe,ing.name, ', ');
    end loop;
    
    return recipe;
END;
$$;


-- Creates the recipe for extra ingredients
CREATE OR REPLACE FUNCTION printExtrasRecipe (pizza_id int, extras varchar(8))
RETURNS varchar
LANGUAGE plpgsql
as
$$
declare
	recipe varchar(200);
    ing record;
    tempextras int[];
BEGIN
    select into tempextras string_to_array(extras, ',')::int[];
    for ing in select DISTINCT i.name, i.id, p.ingredients 
           from Ingredients i
           join Pizza p on i.id = any(string_to_array(p.ingredients, ',')::int[])
           where p.id = pizza_id or i.id = any( string_to_array(extras, ',')::int[])
    loop
        if ing.id = any(tempextras) then
            if ing.id = any(string_to_array(ing.ingredients, ',')::int[]) then
                --raise notice '2x%', ing.name;
                select into recipe CONCAT(recipe,'2x',ing.name,', ');
            else
                --raise notice '%', ing.name;
                select into recipe CONCAT(recipe,ing.name,', ');
            end if;
        else
            --raise notice '%', ing.name;
            select into recipe CONCAT(recipe,ing.name,', ');
        end if;
    end loop;
    
    return recipe;
END;
$$;


-- Execution of the orders recipe print functions
do
$$
declare
    o record;
begin
    for o in select pizza_id, extras 
           from Orders
    loop
        if o.extras = 'null' or o.extras is NULL or o.extras = '' then
            raise notice 'The order recipe is: %--Bon Apetit!--', printSimpleRecipe(o.pizza_id);
        else
            raise notice 'The order recipe is: %--Bon Apetit!--', printExtrasRecipe(o.pizza_id, o.extras);
        end if;
    end loop;
end;
$$;