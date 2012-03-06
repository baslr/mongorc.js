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

test = function() {
	var a = db.favorits.find();

	a.forEach(function(i) {
		i.change.forEach(function(a) {
			b = a.relative;
			if(0 != b)
			{
				print(b);
			}
		});
	});
}

mach = function(mr_latest, mr_previous) {
    var cur 	  = db.getCollection('mr_' +mr_latest).find();
    var per 	  = new cPercenter(cur.count(), 50000);
    var changeCol = db.getCollection('change_' +mr_latest); // change_YYYYMMDD
    var	prevMr	  = db.getCollection('mr_' +mr_previous);

    cur.forEach(function(i) {
		per.step();
        res = prevMr.findOne({_id:i._id}); // hole vom vortag		
	
        if( res ) {
            res2 = i.value.count - res.value.count; // i = heute = 5, res 2 = gestern = 4, 5 - 4 = 1
            
            if( 0 != res2 ) {// ab oder aufstieg
                print('id:' + i._id + ' um ' + res2 + ' aufgestiegen');
                changeCol.insert({_id:i._id, 'change':res2});
            } // if
        } else {
            changeCol.insert({_id:i._id, 'change':0}); // 0 fuer das erste mal dabei
            print('id:' + i._id + ' first time');
        }
    });
    
    changeCol.ensureIndex({change:-1});
    print('ensureIndex for change_' +mr_latest);

	var cur		  = db.getCollection('mr_' +mr_previous).find();
	var latestCol = db.getCollection('mr_' +mr_latest);
	var count	  = 0;
	per.reset(cur.count, 50000);
    
    cur.forEach(function(i)
	{
		per.step();
		res = latestCol.findOne({_id:i._id}); // gucken ob es ihn noch gibt

		if(!res) {
			count++;
			print('id:' + i._id + ' hat keine Favoriten mehr');
		} // if
	});
	print('Insgesamt '+count+' Favoriten gibt es nicht mehr');
}

process = function(changeDate) // aufbauen und updaten der favoriten
{
    var cur		  = db.getCollection('mr_' +changeDate.replace(/\-/g, "")).find(); // ueber alle MapReducten Favoriten
	var per 	  = new cPercenter(cur.count(), 20000);
	var changeCol = db.getCollection('change_' +changeDate.replace(/\-/g, "") );
	
    cur.forEach(function(i)
    {
    	per.step();
        res   = changeCol.findOne({_id:i._id}); // @TODO das vielleicht in die mr tabel mit verbauen? also die relative change
		rel   = (res) ? res.change: 0;
		count = i.value.count;

		a = {_id:i._id, absolute:count, change:[{date     : changeDate,
												 relative : rel }]
		    };

		db.favorits.update({_id:i._id}, { $set:{absolute:count},
										  $push:{change:{date: changeDate, relative:rel}}
										}, a);
    });
}

updateFavorits = function(date, dateBefore) { // YYYY-MM-DD,   YYYYMMDD
	cleanDate = date.replace(/\-/g, ""); // from YYYY-MM-DD to YYYYMMDD

	print('call mapReduce');
	// first mapReduce
	db.getCollection('favorits_' +cleanDate).mapReduce(m, r, {out:'mr_' +cleanDate});

	print('call mach()');	
	// calculate the change collection with todays mapReduce + the mapReduce from the day before
	mach(cleanDate, dateBefore);
	
	print('call process()');
	// insert the change data into the favorits collection
	process(date);
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
	
	this.reset = function(count, mod)
	{
		this.count = count; this.mod = mod; this.current = 0;
	}
	this.reset = function()
	{
		this.current = 0;
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