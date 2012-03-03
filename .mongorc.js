conn = new Mongo();
db = conn.getDB('mr');
print('connected to mr');

m = function()
{
    emit(this.photoId, {count:1});
}

r = function(key, values)
{
    var result = {count:0};
    
    values.forEach(function(value)
    {
        result.count += value.count;
    });

    return result;
}

test = function(a)
{
	a.forEach(function(i)
	{
		printjson(i);
		
		print( i.replace(/\-/g, "") );
	});
}

mach = function(mr_latest, mr_a)
{
    var cur = db.mr_20120302.find();
    var per = new cPercenter(cur.count(), 50000);
    
    cur.forEach(function(i)
    {
		per.step();
		
        res = db.mr_20120301.findOne({_id:i._id}); // hole vom vortag
        
        if(res)
        {
            res2 = i.value.count - res.value.count; // i = heute = 5, res 2 = gestern = 4, 5 - 4 = 1
            
            if(res2 != 0) // ab oder aufstieg
            {
                print('id:' + i._id + ' um ' + res2 + ' aufgestiegen');
                db.change_20120302.insert({_id:i._id, 'change':res2});
            }	
        }
        else
        {
            db.change_20120302.insert({_id:i._id, 'change':0}); // 0 fuer das erste mal dabei
            print('id:' + i._id + ' first time');
        }
    });

	var cur = db.mr_20120301.find();
	var count = 0;
    	cur.forEach(function(i)
	{
		res = db.mr_20120302.findOne({_id:i._id}); // gucken ob es ihn noch gibt

		if(!res)
		{
			count++;
			print('id:' + i._id + ' hat keine Favoriten mehr');
		} // if
	});
	print('Insgesamt '+count+' Favoriten gibt es nicht mehr');
}

process = function(changeDate) // aufbauen und updaten der favoriten
{
    cur = db.mr_20120302.find(); // ueber alle MapReducten Favoriten
	var per = new cPercenter(cur.count(), 20000);
	var changeCol = db.getCollection('change_' + changeDate.replace(/\-/g, "") );
	
    cur.forEach(function(i)
    {
    	per.step();
        res = changeCol.findOne({_id:i._id});
		rel = (res) ? res.change: 0;

		a = {_id:i._id, absolute:i.count, change:[{date : changeDate,
												   relative: rel }]
		    };

		db.favorits.update({_id:i._id}, { $set:{absolute:i.count},
										  $push:{change:{date: changeDate, relative:rel}}
										}, a);
    });
}

changeId = function()
{
	cur = db.mr_20120302.find();
	cur.forEach(function(i)
	{
		a = {_id:parseInt(i._id), count:i.value.count};
		db.mr_20120302_tmp.insert(a);
	});
}

cntFavChange = function()
{
	var a = db.favorits.find();
	var count = 0;
	a.forEach(function(i)
	{
		if(i.change.length)
			count++;
	});
	print(count + ' Favoriten mit change gespeichert');
}

putOut = function()
{
	var cur = db.favorits.find();
	cur.forEach(function(i)
	{
		if(i.change)
		if(1 < i.change.length)
		{
			print(i.change.length);
			printjson(i.change);
		}
	});
}
// === == === == === == === == === == === == === == === == === //
// general classes
function cPercenter(count, mod) // class to calculate progress
{
	this.count   = count;
	this.mod	 = mod;
	this.current = 0;
	
	this.step = function()
	{
		this.current++;
		
		if( 0 == this.current % this.mod)
		{
			print( (100 / this.count * this.current).toFixed(2) + ' Prozent geschafft');
		}
	}
}
// === == === == === == === == === == === == === == === == === //
// functions for the prompt
sc = function() // show collections
{
    out = db.getCollectionNames();
    
    out.forEach(function(value)
    {
        print(value);
    });
}

compactAll = function() // compacts all collections
{
	out = db.getCollectionNames();

	out.forEach(function(value)
	{
		print('Compact '+value);
		col = db.getCollection(value);
		col.runCommand('compact');
	});
}