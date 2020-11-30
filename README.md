# Fed Watch
![](./fed_watch_logo.png)

This is part of the capstone project for the Northwestern University MSDS Program. 
Group Members: Terrence Connelly, Samuel Mori, Ryan Bacastow

# Data Sources 
https://fred.stlouisfed.org/series/WALCL

# Architecture
![](./fed_watch_3.png)
- Cloudwatch triggers lambda handler
- Lambda pulls data and makes calculations
- Lambda writes img output to s3
- Lambda invokes SES message to user emails

# Future Potential Architecture
![](./fed_watch.png)

# Example
![](./model_copy.png)
![](./FedWatchEmailExample2.png)
