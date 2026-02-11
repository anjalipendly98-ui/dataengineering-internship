from PIL import Image, ImageDraw, ImageFont
import os

# Create project root folder path
project_root = os.path.join(os.getcwd(), "..")  # adjusts from scripts folder

# Output path
output_path = os.path.join(project_root, "architecture.png")

# Create image
width, height = 800, 400
img = Image.new("RGB", (width, height), color="white")
draw = ImageDraw.Draw(img)

# Box dimensions
box_w, box_h = 200, 80
margin_x = 50
margin_y = 50

# Coordinates
csv_api_box = (margin_x, margin_y, margin_x+box_w, margin_y+box_h)
etl_box = (width//2 - box_w//2, height//2 - box_h//2, width//2 + box_w//2, height//2 + box_h//2)
gcs_box = (width - box_w - margin_x, margin_y, width - margin_x, margin_y+box_h)

# Draw boxes
draw.rectangle(csv_api_box, outline="black", width=3)
draw.rectangle(etl_box, outline="black", width=3)
draw.rectangle(gcs_box, outline="black", width=3)

# Draw arrows
# CSV/API → ETL
draw.line([(csv_api_box[2], csv_api_box[1]+box_h//2), (etl_box[0], etl_box[1]+box_h//2)], fill="black", width=3)
draw.polygon([(etl_box[0]-10, etl_box[1]+box_h//2-5), (etl_box[0], etl_box[1]+box_h//2), (etl_box[0]-10, etl_box[1]+box_h//2+5)], fill="black")

# ETL → GCS
draw.line([(etl_box[2], etl_box[1]+box_h//2), (gcs_box[0], gcs_box[1]+box_h//2)], fill="black", width=3)
draw.polygon([(gcs_box[0]-10, gcs_box[1]+box_h//2-5), (gcs_box[0], gcs_box[1]+box_h//2), (gcs_box[0]-10, gcs_box[1]+box_h//2+5)], fill="black")

# Add labels
font = None
try:
    font = ImageFont.truetype("arial.ttf", 16)
except:
    font = ImageFont.load_default()

draw.text((csv_api_box[0]+40, csv_api_box[1]+30), "CSV + API", fill="black", font=font)
draw.text((etl_box[0]+30, etl_box[1]+30), "Python ETL Scripts", fill="black", font=font)
draw.text((gcs_box[0]+30, gcs_box[1]+30), "Google Cloud Storage", fill="black", font=font)

# Save image
img.save(output_path)
print(f"✅ Architecture diagram saved at {output_path}")
