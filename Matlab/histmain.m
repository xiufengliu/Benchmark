function histmain(ch, ddir)
%clear all;
%clc;
format long;

homeids=csvread(sprintf('/home/x362liu/datasets/homeids%c.csv', ch));
n=length(homeids);
%n=10;
nbin=10;

fid=fopen(sprintf('results%c.csv',ch), 'wt');
for i=1:n
    [f, errmsg]=fopen(sprintf('/home/x362liu/datasets/%s/%d.csv', ddir, homeids(i)));
    if f<0
        disp(errmsg);
        break;
    end
    data = textscan(f, '%d%s%d%f%f', 'delimiter',',');
    fclose(f);
    x = data{4};
	[ncount, bins]=hist(x', nbin);
	fprintf(fid,'--------%d--------\n', homeids(i));
	for j=1:size(ncount,2)
	   fprintf(fid, '%d,%d\n', bins(j), ncount(j));
	end
end
fclose(fid);
end
