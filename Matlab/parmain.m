function [ ] =parmain(ch, ddir)
%clear all;
%clc;
format long;

homeids=csvread(sprintf('/home/x362liu/datasets/homeids%c.csv', ch));
T=24;

n=length(homeids);
%n=10;
%profile=zeros(testsize,6);
fid=fopen(sprintf('results%c.csv',ch), 'wt');
for i=1:n
    [f, errmsg]=fopen(sprintf('/home/x362liu/datasets/%s/%d.csv',ddir, homeids(i)));
    if f<0
        disp(errmsg);
        break;
    end
    data = textscan(f, '%d%s%d%f%f', 'delimiter',',');
    fclose(f);
    x = data{4};
	phi=PARX(homeids(i),x',24,3);
	for j=1:size(phi,1)
	    fprintf(fid,'%d,%d,%d,%d,%d\n',phi(j,:));
    end
end
fclose(fid);
end
