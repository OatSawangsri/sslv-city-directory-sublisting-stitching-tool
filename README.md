# sslv-city-directory-sublisting-stitching-tool

## Purpose
To identify all sublisting that either got promoted to listing and identify its parent listing. This process only perform when the sublisting at the begin and the end of the segment.

## Dependent
Depend on the segment stitiching to be identify and accessible

## It does not
- not promote or demote the listing in the middle of the segment
- does not modify the current data
- does not perform 100% correct

## Technique in compare address
1. Compare address structure such as 
  - where the character location is simiilar or different
  - Same format example 
    - ``` 123B and 123B ```
    - ``` A99 and B22 ``` 
    - ``` 123 and 333 ```
  - Different format example
    - ```A123 and 123B ```
    - ``` A99 and 22 ``` 
    - ``` 123 and 3A ```
2. Compare which value is closer to each other
  - Example: a = 123 , b = 142, c = 12
    - a is closer to b
  - Example: a = 123 , b = 142, c = A123
    - a is closer to b
    - due to format not align
  - This process is use to determine
    - If one of the address is sublisting
    - Find the similar sublisting in the whole segment
    - Fina all listing that suppost to be listing if the first listing in segment is classified as sublisting
3. Using the listing extraction boundary box, extent, to check the grouping of the listing
  - Compare the target boundary box with the previous one if they are close to each other.