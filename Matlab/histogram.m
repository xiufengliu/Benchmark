function histogram(src, output)
% src: The path of time-series data file
% out: The path of output

	format long;
	[f, errmsg]=fopen(sprintf(src));
	fid=fopen(sprintf(output), 'wt');
	if f<0
		disp(errmsg);
		break;
	end

	data = textscan(f, '%d%s%d%f%f', 'delimiter',',');
	fclose(f);
	x = data{4};
	[ncount, bins]=hist(x', nbin);
	fprintf(fid,'--------%d--------\n', homeids(i));
	for j=1:size(ncount, 2)
	   fprintf(fid, '%d,%d\n', bins(j), ncount(j));
	end
	fclose(fid);
end
