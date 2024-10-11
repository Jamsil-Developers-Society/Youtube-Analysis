import { Box, Grid2, Paper, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import Progress from "./Progress";
import {
  selectCategories,
  selectTotalData,
  selectVideoList,
} from "../services/videoAPI";
import Piechart from "./Piechart";

function TotalPage() {
  // const [arrVideoData, setArrVideoData] = useState([]);
  // const [arrCategoryData, setArrCategoryData] = useState([]);
  const [arrTotalData, setArrTotalData] = useState([]);
  const [progress, setProgress] = useState(true);

  useEffect(() => {
    console.log("데이터 로딩 중..");
    fn();
    setProgress(false);
  }, []);

  const fn = () => {
    // selectVideoList().then((res) => {
    //   setArrVideoData(res.data.rows);
    // });

    // selectCategories().then((res) => {
    //   setArrCategoryData(res.data.rows);
    // });

    selectTotalData().then((res) => {
      // console.log(res?.data);
      const data = JSON.parse(res.data).map((item) => ({
        id: item.category_title,
        label: item.category_title,
        value: item.count,
      }));
      setArrTotalData(data);
      setProgress(false);
    });
  };

  return (
    <>
      <Progress progress={progress} />
      <Box xs={{ p: 2 }}>
        <Grid2 container spacing={2} sx={{ p: 2 }}>
          <Grid2 size={{ xs: 6, md: 12 }}>
            <Paper>
              <Piechart data={arrTotalData} height={"500px"} />
            </Paper>
          </Grid2>
        </Grid2>
        {/* container end */}
      </Box>
    </>
  );
}

export default TotalPage;
