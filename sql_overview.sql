select m.code, m.company, p.price, p.date, p.change, p.moving
from company_map m, prices p,
(select date from prices order by date desc limit 0,1) today, 
(select code from own where user = 'rusiang') o
where 1=1 
and o.code = m.code 
and p.code = o.code 
and p.date = today.date
order by p.moving desc
