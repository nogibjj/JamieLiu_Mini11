SELECT 
    id, 
    country, 
    beer_servings, 
    spirit_servings, 
    wine_servings, 
    total_litres_of_pure_alcohol,
    SUM(beer_servings + spirit_servings + wine_servings) as total_servings
FROM 
    drinks_delta
GROUP BY 
    id, 
    country, 
    beer_servings, 
    spirit_servings, 
    wine_servings, 
    total_litres_of_pure_alcohol
ORDER BY 
    total_servings DESC, 
    total_litres_of_pure_alcohol DESC;
