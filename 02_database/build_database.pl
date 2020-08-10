#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Data::Dumper;
use Archive::Extract;

# Specification:
#   parse through a directory of CSV files
#   unzip each one and import it to a table
my $verbose = 1;
my $firstrun = 0;
my $censusdb = DBI->connect("dbi:SQLite:dbname=census2011.sqlite","","") or die DBI::errstr;
my $landusedb = DBI->connect("dbi:SQLite:dbname=landuse.sqlite","","") or die DBI::errstr;
my $basedir = "../01_data_received";
my $archivedir = "$basedir/meshblocks";
my $censusdir = "$basedir/census_data/long"; # long headers

if ($firstrun) {
	my $result = &make_db();
}
my $result = build_single_table($censusdb, "$censusdir/2011_BCP_ALL_for_AUST_long-header.zip");
my $result = build_single_table($censusdb, "$censusdir/2011_ERP_AU_for_AUST_long-header.zip");
#my $result = build_single_table($censusdb, "$censusdir/2011_IP_ALL_for_AUST_long-header.zip");
$result = build_single_table($censusdb, "$censusdir/2011_PEP_ALL_for_AUST_long-header.zip");
$result = build_single_table($censusdb, "$censusdir/2011_TSP_ALL_for_AUST_long-header.zip");
$result = build_single_table($censusdb, "$censusdir/2011_WPP_ALL_for_AUST_long-header.zip");
$result = build_single_table($censusdb, "$censusdir/2011_XCP_ALL_for_AUST_long-header.zip");

$censusdb->disconnect;
$landusedb->disconnect;

## Subroutines
sub make_db {
	# read in the tables (Mesh blocks and SA1-5)
	# The meshblocks are split up by State, but should be one table for 
	# indexing.  Unlikely that a project would span state boundaries, though.
	# Meshblocks and Boundaries
	drop_all_tables($landusedb);				
	build_all_tables($landusedb, $archivedir);
	# read in the Census Data
	drop_all_tables($censusdb);				
	build_all_tables($censusdb, $censusdir);
}
sub drop_all_tables {
	# get a list of table names from $db and drop them all
	my $db = shift;
	print "Clearing the database because \$firstrun == $firstrun\n";
    my @tables;    
    my $query = querydb($db, "select name from sqlite_master where type='table' order by name", 1);
    # we need to extract the list of tables first - sqlite doesn't like
    # multiple queries at the same time.
    while (my @row = $query->fetchrow_array) {
        push @tables, $row[0];
    }
    dbdo ($db, "BEGIN", 1);
    foreach my $table (@tables) {
        dbdo ($db, "DROP TABLE if Exists [$table]", 1);
    }
    dbdo ($db, "COMMIT", 1);
    return 1;
}
sub querydb {
	# prepare and execute a query
	my $db = shift;
	my $command = shift;
	my $verbose = shift;
	print "\tQUERYDB: $db: $command\n" if $verbose;
    my $query = $db->prepare($command) or die $db->errstr;
    $query->execute or die $query->errstr;
    return $query;
}
sub build_all_tables {
	# read in the data file files
	my $db = shift;
	my $archivedir = shift;
	my @archives = files_in_dir($archivedir);
	foreach my $zipfile (@archives) {
		if ($zipfile =~ /\.zip/){
			#parse the file
			my $result = build_single_table($db, "$archivedir/$zipfile");
		}
	}
	return 1;
}

sub build_single_table {
	my $db = shift;
	my $filepath = shift;
	my @filenames = extract_zip_to_tmp ($filepath);
	foreach my $filename (@filenames) {
		if ($filename =~ /.csv$/) {
			my $tablename = table_name_from_filename($filename);
			print "\t0.Processing \"$filename\" -> [$tablename]...\n" if $verbose;
			open ( my $fh, "<", "./tmp/$filename");
			my $headerline = <$fh>;
			local $/ = "\r\n"; # windows files CRLF
			chomp $headerline;
			#print "\t1.Header:\t$headerline\n" if $verbose;
			my $startpos = tell($fh) ; #Get the position of the second line
			my $firstline = <$fh>;
			# There may not be a second line!
			#$firstline = <$fh>; # second line sometimes has a blank SQM entry
			chomp $firstline;
			if ( $firstline =~ /,$/) {
				# sometimes the first line has no SQM
				$firstline .= "0.0";
			}
			#print "\t2.First:\t$firstline\n" if $verbose;
			my ($censusdbstructure, $fieldnames) = dbstructure_from_headers($headerline, $firstline);
			my $command = "Create Table If Not Exists [$tablename] ($censusdbstructure)";
			dbdo($db, $command, $verbose);
			dbdo($db, "BEGIN", $verbose); # wrap the inserts in a Begin//Commit to speed up
			seek ($fh, $startpos, 0);
			# parse the records and build the database
			my $numrecords = 0;
			while (my $line = <$fh>) {
				chomp $line;
				if ( $line =~ /,$/) {
					# sometimes the first line has no SQM
					$line .= "0.0";
				}
				my $values = sanitise_line_for_input($line);
				$command = "Insert or Replace into [$tablename] ($fieldnames) Values ($values)";
				my $result = dbdo($db, $command, 0);
				if ($result) { $numrecords++;}
			}
			close $fh;
			print "\t4. $numrecords records written to [$tablename].\n";
			# delete the file
			my $result = `rm "./tmp/$filename"`;
			dbdo($db, "COMMIT", $verbose);
			my @fields = split /,/, $headerline;
			dbdo($db, "CREATE UNIQUE INDEX IF NOT EXISTS [$tablename\_index] ON [$tablename] ($fields[0])", $verbose);
		}
	}
}
sub sanitise_line_for_input {
	#take a line like:
	# 80000009499,NOUSUALRESIDENCE,89999949999,8949999,899999499,89499,No usual address (ACT),89999,Special Purpose Codes SA3 (ACT),899,Special Purpose Codes SA4 (ACT),89499,No usual address (ACT),8,Australian Capital Territory
	#and Return:
	# 80000009499, \"NOUSUALRESIDENCE\",89999949999,8949999,899999499,89499,\"No usual address (ACT)\",89999,\Special Purpose Codes SA3 (ACT)\",899,\"Special Purpose Codes SA4 (ACT)\",89499,\"No usual address (ACT)\",8,\"Australian Capital Territory\"
	my $line = shift;
	my $cleanline;
	my @fields = split /,/, $line;
	foreach my $field (@fields) {
		my $type = type_from_data($field);
		if ( $type eq "Text") {
			$cleanline .= "\"$field\",";
		} else {
			$cleanline .= "$field,";
		}
		if ( $type eq "Integer") {
			# check size
			if ($field != 0) {
				my $bits = log($field)/log(2);
				my $bytes = int($bits/8+0.99);
				if ($bytes > 4) { die "$field, $bytes";}
			}
		}
	}
	$cleanline =~ s/,$//;
	return $cleanline;
}
sub dbstructure_from_headers {
	# take the header line, and a data line and figure out the DB Structure
	my $headerline = shift;
	my $firstline = shift;
	my @headers = split /,/, $headerline;
	my @first = split /,/, $firstline;
	my ($dbstructure, $fieldnames);
	foreach my $index (0..($#headers)) {
		# do we need to sanitise the header?
		my $header = $headers[$index];
		if ($header =~ /^[0-9]/) {
			$header = "T$headers[$index]";
		}
		# see what the content is to decide the type
		print "\t\t3.$index: $header: \"$first[$index]\" " if $verbose;
		my $type = type_from_data($first[$index]);
		print ": $type" if $verbose;
		if ($type eq "Integer" || $type eq "Real") {
			if ($first[$index] != 0) {
				my $bits = int(log($first[$index])/log(2));
				my $bytes = int($bits/8+0.99);
				print "($bits bits $bytes bytes)" if $verbose;
			}
		}
		print "\n" if $verbose;
		$dbstructure .= "$header $type";
		$fieldnames .= "$header";
		if ( $index == 0) { $dbstructure .= " Primary Key"; }
		if ( $index < $#headers ) { $dbstructure .= ", "; }
		if ( $index < $#headers ) { $fieldnames .= ", "; }
	}
	return ($dbstructure, $fieldnames);
}

sub type_from_data {
	# take a field and sample data and determine the type
	my $field = shift;
	my $type;
	my $has_text = 0;
	my $has_nums = 0;
	my $has_decs = 0;
	# the decision tree below is a little redundant, might change later
	if ( $field =~ /[A-Za-z()-]+/) { $has_text = 1; }
	if ( $field =~ /\d+/) { $has_nums = 1;	} 
	if ( $field =~ /\./) { $has_decs = 1; }
	if ( $has_text ) { 
		$type = "Text"; 
	} elsif ( $has_decs ) {
		$type = "Real"; 
	} else {
		$type = "Integer";
	}
	return $type;
}

sub table_name_from_filename {
	# take a filename like MB_2011_OT.csv or SA1_2011_AUST.csv
	# and return a tablename line MB_2011 or SA1_2011
	# 2011Census_B40A_AUST_UCL_long.csv -> 2011Census_B40A
	my $filename = shift;
	# strip out the preceeding path
	my @paths = split /\//, $filename;
	print "paths: @paths\n";
	$filename = $paths[-1];
	my ($name, $ext) = split /./, $filename;
	my @parts = split /_/, $filename;
	my $tablename = "$parts[0]\_$parts[1]";
	return $tablename;
}
sub extract_zip_to_tmp {
	# extract a named zipfile to the tmp folder
	# zipfile name includes path
	# return a list of the extracted files
	my $filename = shift;
	print "$filename: " if $verbose;
	my $zip = Archive::Extract->new( archive => "$filename");
	my $ok = $zip->extract( to => "./tmp") or die $zip->error;
	my @files = @{$zip->files}; # has to be after the extraction
	print "extracted $#files files.\n" if $verbose;
	return @files;
}
sub dbdo {
	my $db = shift;
	my $command = shift;
	my $verbose = shift;
	if (length($command) > 1000000) {
		die "$command too long!";
	}
	print "\t$db: ".length($command)." $command\n" if $verbose;
	my $result = $db->do($command) or die $db->errstr . "\nwith: $command\n";
	return $result;
}


sub files_in_dir {
	# return a list of files in a directory
	my $dir = shift;
	opendir (my $dirh, "$dir");
	my @files = readdir ($dirh);
	closedir $dirh;
	return @files;
}
