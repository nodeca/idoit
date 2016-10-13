-- Exec redis commands atomically
--


local params = cjson.decode(KEYS[1])


-- Check checks
for __, check in pairs(params["validate"]) do
  if check["not"] then
    if check["not"][1] == redis.call(unpack(check["not"][2])) then
      return 0
    end
  else
    if check[1] ~= redis.call(unpack(check[2])) then
      return 0
    end
  end
end


-- Eval redis commands
for __, exec in pairs(params["exec"]) do
  redis.call(unpack(exec))
end


return 1
