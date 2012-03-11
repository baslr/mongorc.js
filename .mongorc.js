db = db.getSisterDB('mr');
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

upFavs = function(args) {
    args.forEach(function(pair) {
        var dateLatest   = pair.latest;
        var datePrevious = pair.prev;
        
        if( !db.getCollection('mr_' +dateLatest).count() ) {
        	print('call mapReduce');
        	db.getCollection('favorits_' +dateLatest).mapReduce(m, r, {out:'mr_' +dateLatest}); // mapReduce to group by id
        	print('mapReduce done.');
        } // if
    	
    	print('calculate changes for change collection and insert changes into favorits');
    	var curMr     = db.getCollection('mr_' +dateLatest).find();
    	var prevMr    = db.getCollection('mr_' +datePrevious);
        var changeCol = db.getCollection('change_' +dateLatest); // change_YYYYMMDD
    	var per       = new cPercenter(curMr.count(), 5000);
    	var bChange   = changeCol.count() ? true : false;
    
        if( 0 == db.getCollection('favorits').count() ) {
            print('no favorits collection initial run');
            
            prevMr.find().forEach(function(i) {

            var a = {_id:i._id, absolute:i.value.count, change:[{date     : datePrevious,
                                                                 relative : i.value.cout }]
		            };
		    db.favorits.insert(a);
            }); // forEach
        } // if

    	curMr.forEach(function(i) {
            per.step();
            res = prevMr.findOne({_id:i._id}); // hole vom Vortag
            
            if( res ) { // wenn vorhanden
                res2 = i.value.count - res.value.count; // berrechne veränderung vom vortag
                
                if( 0 != res2 ) { // hat sich etwas getan im Bezug auf den Vortag?
                    print('id:' +i._id +' um ' +res2 +' verändert');

                    if( !bChange ) {
                        changeCol.insert({_id:i._id, change:res2});
                    } // if Change Collection not exists
                    
                    var a = {_id:i._id, absolute:i.value.count, change:[{date : dateLatest,
        								                            relative  : res2 }]
        		            };

                    db.favorits.update({_id:i._id}, { $set:{absolute:i.value.count},
                                                     $push:{change:{date: dateLatest, relative:res2}}
            										}, a);
                } // if 0 != res2
            } else { // das erste mal hier
                print('id:' +i._id +' first time.');
                
                if( !bChange ) {
                    changeCol.insert({_id:i._id, change:i.value.count});                
                }
                
                var a = {_id:i._id, absolute:i.value.count, change:[{date     : dateLatest,
                                                                     relative : i.value.cout }]
    		            };
                
        		db.favorits.update({_id:i._id}, { $set:{absolute:i.value.count},
        										  $push:{change:{date: dateLatest, relative:i.value.count}}
        										}, a); 
            } // else        
    	});	// uebers mapReduce
    }); // forEach
} // upFavs()

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

prntFavs = function(date) { // prints favoris more compact
    db.favorits.find({'change.date':date}).sort({'change.date':-1, 'change.relative':-1}).forEach(function(a) {
        var out = 'id:' +a._id +', abs:' +a.absolute;
        
        a.change.forEach(function(b) {
            out += ', ' +b.date +':' +b.relative;
        })

        print(out);
    });
}

profile = function() { // profiling to the profile collection
    var ts = 0;

    while(true) {
        db.system.profile.find().sort({$natural:-1}).limit(2).forEach(function(a) {
        
                printjson(a);
        }) // forEach
        print('----------------');
        sleep(1000);
    };


/*
    var ts = null;
    while(true) {
        res = db.settings.find({'profiling.active':true});
        
        if( res ) {
            res2 = db.system.profile.find().sort({$natural:-1});
            
            res2.forEach(function(a) {
                time = a.ts.getTime();
            });
        } // if
    }   // while
*/
}











