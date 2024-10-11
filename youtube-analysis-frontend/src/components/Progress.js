import { Backdrop, CircularProgress } from "@mui/material";

const Progress = (props) => {
  const progressing = props.progress;

  return (
    <>
      <Backdrop
        sx={{
          color: "#fff",
          zIndex: (theme) => theme.zIndex.drawer + 1,
        }}
        open={progressing}
      >
        <CircularProgress />
      </Backdrop>
    </>
  );
};

export default Progress;
