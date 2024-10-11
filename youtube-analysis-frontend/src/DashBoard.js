import { Box, Typography } from "@mui/material";
import Piechart from "./components/Piechart";

const DashBoard = () => {
  return (
    <>
      <Box>
        <Typography
          variant="h3"
          sx={{ color: "blue", p: 2, backgroundColor: "black" }}
        >
          Youtube Analysis Dashboard
        </Typography>
      </Box>
      <Box>
        <Piechart
          data={[
            { id: "cola", value: 324 },
            { id: "cidar", value: 88 },
            { id: "fanta", value: 221 },
          ]}
        />
      </Box>
    </>
  );
};

export default DashBoard;
